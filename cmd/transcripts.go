package cmd

import (
	"log/slog"
	"os"

	"github.com/BushSchoolIT/extractor/blackbaud"
	"github.com/BushSchoolIT/extractor/database"
	"github.com/spf13/cobra"
)

func Transcripts(cmd *cobra.Command, args []string) {
	// load config and blackbaud API
	api, err := blackbaud.NewBBApiConnector(fAuthFile)
	if err != nil {
		slog.Error("Unable to access blackbaud api", slog.Any("error", err))
		os.Exit(1)
	}
	config, err := loadConfig(fConfigFile)
	if err != nil {
		slog.Error("Unable to load config", slog.Any("error", err))
		os.Exit(1)
	}
	db, err := database.Connect(config.Postgres)
	if err != nil {
		slog.Error("Unable to connect to DB", slog.Any("error", err))
		os.Exit(1)
	}
	defer db.Close()
	// actual logic

	t := []blackbaud.ProcessedRow{}
	for _, id := range config.TranscriptListIDs {
		slog.Info("Processing List", slog.String("id", id))
		for page := 1; ; page++ {
			parsed, err := api.GetAdvancedList(id, page)
			if err != nil {
				slog.Error("Unable to get advanced list", slog.String("id", id), slog.Int("page", page))
				continue
			}
			if len(parsed.Results.Rows) == 0 {
				break // No more data
			}

			slog.Info("Collecting Data From Page", slog.Int("page", page))
			t = append(t, blackbaud.ProcessBlackbaudRows(parsed.Results.Rows, transcriptProcess)...)
		}
	}
	slog.Info("Import Complete")

	slog.Info("Starting Database transformations")
	err = db.TranscriptOps(t, api.StartYear, api.EndYear)
	if err != nil {
		slog.Error("Unable to complete transcript operations", slog.Any("error", err))
		os.Exit(1)
	}
}

func transcriptProcess(k string, v any) (string, any, bool) {
	if k == "grade_id" && v == nil {
		v = 999999
	}
	return k, v, true
}
