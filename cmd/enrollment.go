package cmd

import (
	"log/slog"

	"github.com/BushSchoolIT/extractor/blackbaud"
	"github.com/BushSchoolIT/extractor/database"
	"github.com/spf13/cobra"
)

func Enrollment(cmd *cobra.Command, args []string) error {
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
	slog.Info("Processing enrolled List", slog.String("id", config.EnrollmentListIDs.Enrolled))

	enrolled, err := blackbaud.ProcessList(api, config.EnrollmentListIDs.Enrolled)
	if err != nil {
		slog.Error("Unable to get enrollment data", slog.Any("error", err))
		return err
	}
	departed, err := blackbaud.ProcessList(api, config.EnrollmentListIDs.Departed)
	if err != nil {
		slog.Error("Unable to get departed data", slog.Any("error", err))
		return err
	}
	err = db.EnrollmentOps(enrolled, departed)
	if err != nil {
		slog.Error("Unable to complete enrollment database operations", slog.Any("error", err))
	}
	slog.Info("Import Complete")
	return nil
}
