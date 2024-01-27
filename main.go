package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bwmarrin/discordgo"
	"github.com/gen2brain/beeep"
	"github.com/samber/lo"
	"github.com/urfave/cli/v2"
)

// recordtxt is a map of card ID to player name file.
//
// Example:
// cards = dict()
// with open(record_path, 'r') as f:
//     lines = [line.strip('\n') for line in filter(lambda x: x != '\n', f.readlines())]
//     for line in lines:
//         card_num, name = line.split()
//         cards[name] = card_num

func parseRecordTxt(path string) (map[string]string, error) {
	cards := make(map[string]string)

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Split(line, " ")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid line in record.txt: %s", line)
		}

		cardNum := parts[0]
		name := parts[1]

		cards[name] = cardNum
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return cards, nil
}

var cards map[string]string

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	app := &cli.App{
		Name:  "aimeswitcher",
		Usage: "AIME Switcher",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "token",
				Usage:    "Discord Bot Token",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "appid",
				Usage:    "Discord App ID",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "name",
				Usage: "Game name",
				Value: "maimai",
			},
			&cli.StringFlag{
				Name:  "place",
				Usage: "Game place",
				Value: "RhythmROC",
			},
			&cli.PathFlag{
				Name:     "aimetxt-path",
				Usage:    "Path to the aime.txt file",
				Required: true,
			},
			&cli.PathFlag{
				Name:     "recordtxt-path",
				Usage:    "Path to the record.txt file",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "mysql-dburl",
				Usage: "MySQL DB URL. Example: root:password@tcp(localhost:3306)/aime",
			},
			&cli.StringFlag{
				Name:  "r2-accountid",
				Usage: "R2 Account ID",
			},
			&cli.StringFlag{
				Name:  "r2-bucket",
				Usage: "R2 Bucket",
			},
			&cli.StringFlag{
				Name:  "r2-accountkeyid",
				Usage: "R2 Account Key ID",
			},
			&cli.StringFlag{
				Name:  "r2-accountkey",
				Usage: "R2 Account Key",
			},
		},
		Action: Start,
	}

	if err := app.Run(os.Args); err != nil {
		log.Println(err)
	}

	log.Println("Program has exited. Waiting for signal...")
	<-make(chan struct{})
}

type CommandHandlerCtx struct {
	c *cli.Context
}

func redactedCardNum(cardNum string) string {
	if len(cardNum) < 4 {
		return cardNum
	}

	return fmt.Sprintf("*%s", cardNum[len(cardNum)-4:])
}

func Start(c *cli.Context) error {
	if c.String("mysql-dburl") != "" {
		StartDBUpdater(c)
	}

	recordtxtPath := c.String("recordtxt-path")

	records, err := parseRecordTxt(recordtxtPath)
	if err != nil {
		return err
	}
	cards = records

	dg, err := discordgo.New("Bot " + c.String("token"))
	if err != nil {
		return err
	}

	if err := dg.Open(); err != nil {
		return err
	}

	// add presence

	commands := []*discordgo.ApplicationCommand{
		{
			Name:        "switch",
			Description: fmt.Sprintf("Switch active AIME of %s", c.String("name")),
			Options: []*discordgo.ApplicationCommandOption{
				{
					Name:         "card",
					Autocomplete: true,
					Type:         discordgo.ApplicationCommandOptionString,
					Description:  "AIME card",
					Required:     true,
				},
			},
		},
		{
			Name:        "whoami",
			Description: fmt.Sprintf("Get current active AIME of %s", c.String("name")),
		},
	}

	if _, err = dg.ApplicationCommandBulkOverwrite(c.String("appid"), "", commands); err != nil {
		return err
	}

	hCtx := &CommandHandlerCtx{c: c}

	handlers := map[string]func(s *discordgo.Session, i *discordgo.InteractionCreate){
		"switch": hCtx.CommandSwitch,
		"whoami": hCtx.CommandWhoami,
	}

	dg.AddHandler(func(s *discordgo.Session, i *discordgo.InteractionCreate) {
		defer func() {
			if err := recover(); err != nil {
				log.Println("recovered from panic:", err)
			}
		}()

		name := i.ApplicationCommandData().Name
		if i.Type == discordgo.InteractionApplicationCommandAutocomplete {
			switch name {
			case "switch":
				choices := make([]*discordgo.ApplicationCommandOptionChoice, 0, len(cards))
				for name, cardNum := range cards {
					choices = append(choices, &discordgo.ApplicationCommandOptionChoice{
						Name:  fmt.Sprintf("%s (%s)", name, redactedCardNum(cardNum)),
						Value: cardNum,
					})
				}

				log.Println("autocomplete: responding with choices", choices)

				lo.Must0(s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
					Type: discordgo.InteractionApplicationCommandAutocompleteResult,
					Data: &discordgo.InteractionResponseData{
						Choices: choices,
					},
				}))
			default:
				lo.Must0(s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
					Type: discordgo.InteractionResponseChannelMessageWithSource,
					Data: &discordgo.InteractionResponseData{
						Content: "Unknown autocomplete command",
					},
				}))
			}
		} else {
			log.Println("command: got command", name, "from", i.Member.User.Username)
			if handler, ok := handlers[name]; ok {
				handler(s, i)
			} else {
				lo.Must0(s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
					Type: discordgo.InteractionResponseChannelMessageWithSource,
					Data: &discordgo.InteractionResponseData{
						Content: "Unknown command",
					},
				}))
			}
		}
	})

	log.Println("Bot is running!")
	<-make(chan struct{})

	return nil
}

func (h *CommandHandlerCtx) CommandSwitch(s *discordgo.Session, i *discordgo.InteractionCreate) {
	// write to aime.txt
	cardNum := i.ApplicationCommandData().Options[0].StringValue()
	if err := os.WriteFile(h.c.String("aimetxt-path"), []byte(cardNum), 0o644); err != nil {
		lo.Must0(s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: fmt.Sprintf("Failed to write to aime.txt: %v", err),
			},
		}))
	}

	cardName := "(unknown)"
	for name, num := range cards {
		if num == string(cardNum) {
			cardName = name
			break
		}
	}

	message := fmt.Sprintf("Switched active AIME on **%s** to **%s** (`%s`)", h.c.String("name"), cardName, cardNum)

	log.Println(message)

	lo.Must0(s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: message,
		},
	}))

	lo.Must0(beeep.Notify(fmt.Sprintf("%s AIME Switched", h.c.String("name")), message, ""))
}

func (h *CommandHandlerCtx) CommandWhoami(s *discordgo.Session, i *discordgo.InteractionCreate) {
	// read from aime.txt
	cardNum, err := os.ReadFile(h.c.String("aimetxt-path"))
	if err != nil {
		lo.Must0(s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: fmt.Sprintf("Failed to read from aime.txt: %v", err),
			},
		}))
	}

	cardName := "(unknown)"
	for name, num := range cards {
		if num == string(cardNum) {
			cardName = name
			break
		}
	}

	log.Println("whoami: responding with", cardName, cardNum)

	lo.Must0(s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: fmt.Sprintf("Active AIME on **%s** is **%s** (`%s`)", h.c.String("name"), cardName, cardNum),
		},
	}))
}
