package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/BushSchoolIT/extractor/blackbaud"
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
	enrollmentCmd = &cobra.Command{
		Use:   "enrollment",
		Short: "Extracts enrollment info from blackbaud and imports into the database",
		Run:   Enrollment,
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
		Enrolled string `json:"enrolled"`
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

// generic list process function
func processList(api *blackbaud.BBAPIConnector, id string) blackbaud.UnorderedTable {
	t := blackbaud.UnorderedTable{}
	for page := 1; ; page++ {
		parsed, err := api.GetAdvancedList(id, page)
		if err != nil {
			slog.Error("Unable to get advanced list", slog.String("id", id), slog.Int("page", page))
			continue
		}
		if len(parsed.Results.Rows) == 0 {
			break // No more data
		}
		if len(t.Columns) == 0 {
			t.Columns = getColumns(parsed.Results.Rows[0])
		}

		slog.Info("Collecting Data From Page", slog.Int("page", page), slog.String("id", id))
		for _, row := range parsed.Results.Rows {
			newRow := []any{}
			for _, col := range row.Columns {
				newRow = append(newRow, col.Value)
			}
			t.Rows = append(t.Rows, newRow)
		}
	}
	return t
}
