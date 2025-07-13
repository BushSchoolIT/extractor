package cmd

import (
	"log/slog"
	"os"

	"github.com/BushSchoolIT/extractor/blackbaud"
	"github.com/BushSchoolIT/extractor/database"
	"github.com/spf13/cobra"
)

func Comments(cmd *cobra.Command, args []string) {
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

	t := blackbaud.UnorderedTable{}
	for page := 1; ; page++ {
		parsed, err := api.GetAdvancedList(config.TranscriptCommentsID, page)
		if err != nil {
			slog.Error("Unable to get advanced list", slog.String("id", config.TranscriptCommentsID), slog.Int("page", page))
			continue
		}
		if len(parsed.Results.Rows) == 0 {
			break // No more data
		}
		if len(t.Columns) == 0 {
			t.Columns = getColumns(parsed.Results.Rows[0])
		}

		slog.Info("Collecting Data From Page", slog.Int("page", page), slog.String("id", config.TranscriptCommentsID))
		for _, row := range parsed.Results.Rows {
			newRow := []any{}
			for _, col := range row.Columns {
				newRow = append(newRow, col.Value)
			}
			t.Rows = append(t.Rows, newRow)
		}
	}

	slog.Info("Import Complete")

	slog.Info("Starting Database transformations")
	err = db.TranscriptCommentOps(t)
	if err != nil {
		slog.Error("Unable to complete transcript operations", slog.Any("error", err))
		os.Exit(1)
	}
	slog.Info("Finish Database transformations")
}
