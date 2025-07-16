package cmd

import (
	"log/slog"

	"github.com/BushSchoolIT/extractor/blackbaud"
	"github.com/BushSchoolIT/extractor/database"
	"github.com/spf13/cobra"
)

func Comments(cmd *cobra.Command, args []string) error {
	// load config and blackbaud API
	api, err := blackbaud.NewBBApiConnector(fAuthFile)
	if err != nil {
		slog.Error("Unable to access blackbaud api", slog.Any("error", err))
		return err
	}
	config, err := loadConfig(fConfigFile)
	if err != nil {
		slog.Error("Unable to load config", slog.Any("error", err))
		return err
	}
	db, err := database.Connect(config.Postgres)
	if err != nil {
		slog.Error("Unable to connect to DB", slog.Any("error", err))
		return err
	}
	defer db.Close()
	// actual logic
	t, err := blackbaud.ProcessList(api, config.TranscriptCommentsID)
	if err != nil {
		slog.Error("Unable to complete transcript operations", slog.Any("error", err))
		return err
	}
	slog.Info("Import Complete")

	slog.Info("Starting Database transformations")
	err = db.TranscriptCommentOps(t)
	if err != nil {
		slog.Error("Unable to complete transcript operations", slog.Any("error", err))
		return err
	}
	slog.Info("Finish Database transformations")
	return nil
}
