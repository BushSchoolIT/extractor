package cmd

import (
	"log/slog"

	"github.com/BushSchoolIT/extractor/database"
	"github.com/spf13/cobra"
)

func Gpa(cmd *cobra.Command, args []string) error {
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
	slog.Info("Doing GPA calculations")
	err = db.GpaCalculation()
	if err != nil {
		slog.Error("Unable to do GPA calculations", slog.Any("error", err))
		return err
	}
	slog.Info("Finished GPA calculations")
	return nil
}
