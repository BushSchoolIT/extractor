package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/BushSchoolIT/extractor/database"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "bbextract",
		Short: "bbextract is the successor to BlackBaudExtractor rewritten in Go",
	}
	transcriptCmd = &cobra.Command{
		Use:   "transcripts",
		Short: "Extracts transcript info from blackbaud and imports it into the database, does GPA calculations",
		Run:   Transcripts,
	}
	gpaCmd = &cobra.Command{
		Use:   "gpa",
		Short: "Runs GPA ETL independently (this runs automatically when running transcripts but is helpful for testing/quick refreshes)",
		Run:   Gpa,
	}
	commentsCmd = &cobra.Command{
		Use:   "comments",
		Short: "Extracts transcript comments from blackbaud and imports it into the database",
		Run:   Comments,
	}
	parentsCmd = &cobra.Command{
		Use:   "parents",
		Short: "Extracts parent info from blackbaud and imports it into the database for mailing info",
		Run:   Parents,
	}
	attendanceCmd = &cobra.Command{
		Use:   "attendance",
		Short: "Extracts attendance info from blackbaud and imports it into the database",
		Run:   Attendance,
	}
	fLogFile    string
	fLogLevel   string
	fConfigFile string
	fAuthFile   string
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(transcriptCmd)
	rootCmd.AddCommand(parentsCmd)
	rootCmd.AddCommand(attendanceCmd)
	rootCmd.AddCommand(commentsCmd)
	rootCmd.AddCommand(gpaCmd)
	rootCmd.PersistentFlags().StringVar(&fConfigFile, "config", "config.json", "config file containing list IDs")
	rootCmd.PersistentFlags().StringVar(&fAuthFile, "auth", "bb_auth.json", "authconfig for blackbaud")
}

type Config struct {
	ParentsID            string          `json:"parents_list_id"`
	TranscriptListIDs    []string        `json:"transcript_list_ids"`
	Postgres             database.Config `json:"postgres"`
	TranscriptCommentsID string          `json:"transcript_comments_id"`
	Attendance           struct {
		LevelIDs []string `json:"level_ids"`
	} `json:"attendance"`
	EnrollmentListIDs struct {
		Departed string `json:"departed"`
		Current  string `json:"current"`
	} `json:"enrollment_list_ids"`
}

func loadConfig(configPath string) (Config, error) {
	var config Config
	f, err := os.Open(configPath)
	if err != nil {
		return config, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return config, err
	}
	return config, nil
}
