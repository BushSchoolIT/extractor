package cmd

import (
	// "log/slog"
	// "os"

	// "github.com/BushSchoolIT/extractor/blackbaud"
	// "github.com/BushSchoolIT/extractor/database"
	"github.com/spf13/cobra"
)

func Enrollment(cmd *cobra.Command, args []string) {
	// // load config and blackbaud API
	// api, err := blackbaud.NewBBApiConnector(fAuthFile)
	// if err != nil {
	// 	slog.Error("Unable to access blackbaud api", slog.Any("error", err))
	// 	os.Exit(1)
	// }
	// config, err := loadConfig(fConfigFile)
	// if err != nil {
	// 	slog.Error("Unable to load config", slog.Any("error", err))
	// 	os.Exit(1)
	// }
	// db, err := database.Connect(config.Postgres)
	// if err != nil {
	// 	slog.Error("Unable to connect to DB", slog.Any("error", err))
	// 	os.Exit(1)
	// }
	// defer db.Close()
	// // actual logic
	// slog.Info("Processing current List", slog.String("id", config.EnrollmentListIDs.Current))
	// slog.Info("Import Complete")
}
