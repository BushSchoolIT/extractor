package cmd

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/BushSchoolIT/extractor/blackbaud"
	"github.com/spf13/cobra"
)

func Transcripts(cmd *cobra.Command, args []string) {
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

	client := &http.Client{}
	for _, id := range config.TranscriptListIDs {
		req, err := api.NewRequest(http.MethodGet, blackbaud.LISTS_API+id+"?page="+"0", nil)
		if err != nil {
			slog.Error("Unable to create request", slog.Any("error", err))
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			slog.Error("Unable to access blackbaud api", slog.Any("error", err))
			continue
		}
		slog.Info("Request Succeeded!", slog.Any("response", resp))
	}

}
