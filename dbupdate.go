package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

const RecordVersion = 1

func StartDBUpdater(c *cli.Context) {
	go func() {
		dbu := &DBUpdater{
			Place: c.String("place"),
			Game:  c.String("name"),

			MySqlDBURL:     c.String("mysql-dburl"),
			R2AccountID:    c.String("r2-accountid"),
			R2Bucket:       c.String("r2-bucket"),
			R2AccountKeyID: c.String("r2-accountkeyid"),
			R2AccountKey:   c.String("r2-accountkey"),
		}
		if err := dbu.Start(); err != nil {
			log.Fatalln(err)
		}
	}()
}

type DBUpdater struct {
	Place string
	Game  string

	MySqlDBURL     string
	R2AccountID    string
	R2Bucket       string
	R2AccountKeyID string
	R2AccountKey   string

	lastContentSha256 string
}

func (d *DBUpdater) Start() error {
	log.Println("mysql db url has been provided and thus db updater has been enabled")
	if err := d.update(); err != nil {
		// initial update
		return err
	}

	go func() {
		// update after 1 minute of each previous update
		for {
			time.Sleep(1 * time.Minute)

			if err := d.update(); err != nil {
				log.Println(err)
			}
		}
	}()
	return nil
}

type Content struct {
	RatingRecords  []*RatingRecord  `json:"rating_records"`
	ProfileDetails []*ProfileDetail `json:"profile_details"`
	Version        int              `json:"version"`
}

func (d *DBUpdater) update() error {
	bucketName := d.R2Bucket
	accountId := d.R2AccountID
	accessKeyId := d.R2AccountKeyID
	accessKeySecret := d.R2AccountKey

	u := fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountId)

	log.Println("updating db: formatted r2 url:", u)

	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: u,
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(r2Resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyId, accessKeySecret, "")),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		return errors.Wrap(err, "failed to load aws config")
	}

	client := s3.NewFromConfig(cfg)

	db, err := sql.Open("mysql", d.MySqlDBURL)
	if err != nil {
		return errors.Wrap(err, "failed to open mysql db")
	}

	content, err := d.getContent(db)
	if err != nil {
		return errors.Wrap(err, "failed to get content")
	}

	// marshal to json
	b, err := json.Marshal(content)
	if err != nil {
		return errors.Wrap(err, "failed to marshal content")
	}

	// calculate sha256
	currentSha := fmt.Sprintf("%x", sha256.Sum256(b))
	if currentSha == d.lastContentSha256 {
		log.Println("no update: sha256 is same as previous:", currentSha)
		// no update
		return nil
	}

	// new string buffer
	buf := bytes.NewBuffer(b)

	log.Println("db updating:", currentSha)

	// upload to s3
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(fmt.Sprintf("ratings-v0/%s/%s.json", d.Place, d.Game)),
		Body:        buf,
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return errors.Wrap(err, "failed to upload to s3")
	}

	// update last sha256
	d.lastContentSha256 = currentSha

	log.Println("db updated:", currentSha)

	return nil
}

type RatingRecord struct {
	ID                int             `json:"id"`
	User              int             `json:"user"`
	Version           int             `json:"version"`
	Rating            int             `json:"rating"`
	RatingList        json.RawMessage `json:"ratingList"`
	NewRatingList     json.RawMessage `json:"newRatingList"`
	NextRatingList    json.RawMessage `json:"nextRatingList"`
	NextNewRatingList json.RawMessage `json:"nextNewRatingList"`
	Udemae            json.RawMessage `json:"udemae"`
}

type ProfileDetail struct {
	ID                       int64           `json:"id"`
	User                     int64           `json:"user"`
	Version                  int64           `json:"version"`
	UserName                 string          `json:"userName"`
	IsNetMember              int64           `json:"isNetMember"`
	IconID                   int64           `json:"iconId"`
	PlateID                  int64           `json:"plateId"`
	TitleID                  int64           `json:"titleId"`
	PartnerID                int64           `json:"partnerId"`
	FrameID                  int64           `json:"frameId"`
	SelectMapID              int64           `json:"selectMapId"`
	TotalAwake               int64           `json:"totalAwake"`
	GradeRating              int64           `json:"gradeRating"`
	MusicRating              int64           `json:"musicRating"`
	PlayerRating             int64           `json:"playerRating"`
	HighestRating            int64           `json:"highestRating"`
	GradeRank                int64           `json:"gradeRank"`
	ClassRank                int64           `json:"classRank"`
	CourseRank               int64           `json:"courseRank"`
	CharaSlot                json.RawMessage `json:"charaSlot"`
	CharaLockSlot            json.RawMessage `json:"charaLockSlot"`
	ContentBit               int64           `json:"contentBit"`
	PlayCount                int64           `json:"playCount"`
	CurrentPlayCount         int64           `json:"currentPlayCount"`
	RenameCredit             int64           `json:"renameCredit"`
	MapStock                 int64           `json:"mapStock"`
	EventWatchedDate         string          `json:"eventWatchedDate"`
	LastGameID               string          `json:"lastGameId"`
	LastROMVersion           string          `json:"lastRomVersion"`
	LastDataVersion          string          `json:"lastDataVersion"`
	LastLoginDate            string          `json:"lastLoginDate"`
	LastPairLoginDate        string          `json:"lastPairLoginDate"`
	LastPlayDate             string          `json:"lastPlayDate"`
	LastTrialPlayDate        string          `json:"lastTrialPlayDate"`
	LastPlayCredit           int64           `json:"lastPlayCredit"`
	LastPlayMode             int64           `json:"lastPlayMode"`
	LastPlaceID              int64           `json:"lastPlaceId"`
	LastPlaceName            string          `json:"lastPlaceName"`
	LastAllNetID             int64           `json:"lastAllNetId"`
	LastRegionID             int64           `json:"lastRegionId"`
	LastRegionName           string          `json:"lastRegionName"`
	LastClientID             string          `json:"lastClientId"`
	LastCountryCode          string          `json:"lastCountryCode"`
	LastSelectEMoney         int64           `json:"lastSelectEMoney"`
	LastSelectTicket         int64           `json:"lastSelectTicket"`
	LastSelectCourse         int64           `json:"lastSelectCourse"`
	LastCountCourse          int64           `json:"lastCountCourse"`
	FirstGameID              string          `json:"firstGameId"`
	FirstROMVersion          string          `json:"firstRomVersion"`
	FirstDataVersion         string          `json:"firstDataVersion"`
	FirstPlayDate            string          `json:"firstPlayDate"`
	CompatibleCMVersion      string          `json:"compatibleCmVersion"`
	DailyBonusDate           string          `json:"dailyBonusDate"`
	DailyCourseBonusDate     string          `json:"dailyCourseBonusDate"`
	PlayVsCount              int64           `json:"playVsCount"`
	PlaySyncCount            int64           `json:"playSyncCount"`
	WinCount                 int64           `json:"winCount"`
	HelpCount                int64           `json:"helpCount"`
	ComboCount               int64           `json:"comboCount"`
	TotalDeluxscore          int64           `json:"totalDeluxscore"`
	TotalBasicDeluxscore     int64           `json:"totalBasicDeluxscore"`
	TotalAdvancedDeluxscore  int64           `json:"totalAdvancedDeluxscore"`
	TotalExpertDeluxscore    int64           `json:"totalExpertDeluxscore"`
	TotalMasterDeluxscore    int64           `json:"totalMasterDeluxscore"`
	TotalReMasterDeluxscore  int64           `json:"totalReMasterDeluxscore"`
	TotalSync                int64           `json:"totalSync"`
	TotalBasicSync           int64           `json:"totalBasicSync"`
	TotalAdvancedSync        int64           `json:"totalAdvancedSync"`
	TotalExpertSync          int64           `json:"totalExpertSync"`
	TotalMasterSync          int64           `json:"totalMasterSync"`
	TotalReMasterSync        int64           `json:"totalReMasterSync"`
	TotalAchievement         int64           `json:"totalAchievement"`
	TotalBasicAchievement    int64           `json:"totalBasicAchievement"`
	TotalAdvancedAchievement int64           `json:"totalAdvancedAchievement"`
	TotalExpertAchievement   int64           `json:"totalExpertAchievement"`
	TotalMasterAchievement   int64           `json:"totalMasterAchievement"`
	TotalReMasterAchievement int64           `json:"totalReMasterAchievement"`
	PlayerOldRating          int64           `json:"playerOldRating"`
	PlayerNewRating          int64           `json:"playerNewRating"`
	DateTime                 int64           `json:"dateTime"`
	BanState                 int64           `json:"banState"`
}

func (d *DBUpdater) getContent(db *sql.DB) (*Content, error) {
	ratingRecordRows, err := db.Query("SELECT id, user, version, rating, ratingList, newRatingList, nextRatingList, nextNewRatingList, udemae FROM mai2_profile_rating ORDER BY id ASC")
	if err != nil {
		return nil, errors.Wrap(err, "failed to query rating records")
	}
	defer ratingRecordRows.Close()

	var ratingRecords []*RatingRecord
	for ratingRecordRows.Next() {
		var r RatingRecord
		if err := ratingRecordRows.Scan(&r.ID, &r.User, &r.Version, &r.Rating, &r.RatingList, &r.NewRatingList, &r.NextRatingList, &r.NextNewRatingList, &r.Udemae); err != nil {
			return nil, err
		}
		ratingRecords = append(ratingRecords, &r)
	}

	profileDetailRows, err := db.Query("SELECT id, user, version, userName, isNetMember, iconId, plateId, titleId, partnerId, frameId, selectMapId, totalAwake, gradeRating, musicRating, playerRating, highestRating, gradeRank, classRank, courseRank, charaSlot, charaLockSlot, contentBit, playCount, currentPlayCount, renameCredit, mapStock, eventWatchedDate, lastGameId, lastRomVersion, lastDataVersion, lastLoginDate, lastPairLoginDate, lastPlayDate, lastTrialPlayDate, lastPlayCredit, lastPlayMode, lastPlaceId, lastPlaceName, lastAllNetId, lastRegionId, lastRegionName, lastClientId, lastCountryCode, lastSelectEMoney, lastSelectTicket, lastSelectCourse, lastCountCourse, firstGameId, firstRomVersion, firstDataVersion, firstPlayDate, compatibleCmVersion, dailyBonusDate, dailyCourseBonusDate, playVsCount, playSyncCount, winCount, helpCount, comboCount, totalDeluxscore, totalBasicDeluxscore, totalAdvancedDeluxscore, totalExpertDeluxscore, totalMasterDeluxscore, totalReMasterDeluxscore, totalSync, totalBasicSync, totalAdvancedSync, totalExpertSync, totalMasterSync, totalReMasterSync, totalAchievement, totalBasicAchievement, totalAdvancedAchievement, totalExpertAchievement, totalMasterAchievement, totalReMasterAchievement, playerOldRating, playerNewRating, dateTime, banState FROM mai2_profile_detail ORDER BY id ASC")
	if err != nil {
		return nil, err
	}
	defer profileDetailRows.Close()

	var profileDetails []*ProfileDetail
	for profileDetailRows.Next() {
		var p ProfileDetail
		if err := profileDetailRows.Scan(&p.ID, &p.User, &p.Version, &p.UserName, &p.IsNetMember, &p.IconID, &p.PlateID, &p.TitleID, &p.PartnerID, &p.FrameID, &p.SelectMapID, &p.TotalAwake, &p.GradeRating, &p.MusicRating, &p.PlayerRating, &p.HighestRating, &p.GradeRank, &p.ClassRank, &p.CourseRank, &p.CharaSlot, &p.CharaLockSlot, &p.ContentBit, &p.PlayCount, &p.CurrentPlayCount, &p.RenameCredit, &p.MapStock, &p.EventWatchedDate, &p.LastGameID, &p.LastROMVersion, &p.LastDataVersion, &p.LastLoginDate, &p.LastPairLoginDate, &p.LastPlayDate, &p.LastTrialPlayDate, &p.LastPlayCredit, &p.LastPlayMode, &p.LastPlaceID, &p.LastPlaceName, &p.LastAllNetID, &p.LastRegionID, &p.LastRegionName, &p.LastClientID, &p.LastCountryCode, &p.LastSelectEMoney, &p.LastSelectTicket, &p.LastSelectCourse, &p.LastCountCourse, &p.FirstGameID, &p.FirstROMVersion, &p.FirstDataVersion, &p.FirstPlayDate, &p.CompatibleCMVersion, &p.DailyBonusDate, &p.DailyCourseBonusDate, &p.PlayVsCount, &p.PlaySyncCount, &p.WinCount, &p.HelpCount, &p.ComboCount, &p.TotalDeluxscore, &p.TotalBasicDeluxscore, &p.TotalAdvancedDeluxscore, &p.TotalExpertDeluxscore, &p.TotalMasterDeluxscore, &p.TotalReMasterDeluxscore, &p.TotalSync, &p.TotalBasicSync, &p.TotalAdvancedSync, &p.TotalExpertSync, &p.TotalMasterSync, &p.TotalReMasterSync, &p.TotalAchievement, &p.TotalBasicAchievement, &p.TotalAdvancedAchievement, &p.TotalExpertAchievement, &p.TotalMasterAchievement, &p.TotalReMasterAchievement, &p.PlayerOldRating, &p.PlayerNewRating, &p.DateTime, &p.BanState); err != nil {
			return nil, err
		}
		profileDetails = append(profileDetails, &p)
	}

	return &Content{
		RatingRecords:  ratingRecords,
		ProfileDetails: profileDetails,
		Version:        RecordVersion,
	}, nil
}
