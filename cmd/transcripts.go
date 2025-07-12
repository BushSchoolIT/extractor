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

	t := blackbaud.UnorderedTable{}
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
			if len(t.Columns) == 0 {
				t.Columns = getColumns(parsed.Results.Rows[0])
			}

			slog.Info("Collecting Data From Page", slog.Int("page", page))
			for _, row := range parsed.Results.Rows {
				newRow := []any{}
				for _, col := range row.Columns {
					if col.Name == "grade_id" && col.Value == nil {
						col.Value = 999999
					}
					newRow = append(newRow, col.Value)
				}
				t.Rows = append(t.Rows, newRow)
			}
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

func getColumns(row blackbaud.Row) []string {
	columns := []string{}
	for _, col := range row.Columns {
		columns = append(columns, col.Name)
	}
	return columns
}
