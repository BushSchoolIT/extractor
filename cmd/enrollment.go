package cmd

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/BushSchoolIT/extractor/blackbaud"
	"github.com/BushSchoolIT/extractor/database"
	"github.com/spf13/cobra"
)

func Enrollment(cmd *cobra.Command, args []string) {
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
	db.TranscriptCleanup(api.EndYear)

	slog.Info("Processing current List", slog.String("id", config.EnrollmentListIDs.Current))
	processEnrollmentList(config.EnrollmentListIDs.Current, api, db.InsertEnrollmentInfo)
	slog.Info("Import Complete")
}

func processEnrollmentList(id string, api *blackbaud.BBAPIConnector, insert func([]string, []any) error) {
	for page := 1; ; page++ {
		req, err := api.NewRequest(http.MethodGet, blackbaud.AdvancedListApi(id, page), nil)
		if err != nil {
			slog.Error("Unable to create request", slog.Any("error", err))
			continue
		}
		resp, err := api.Client.Do(req)
		if err != nil {
			slog.Error("Unable to access blackbaud api", slog.Any("error", err))
			continue
		}
		body, err := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			slog.Error("Blackbaud API returned unexpected status code", slog.Any("code", resp.StatusCode), slog.String("body", string(body)))
			continue
		}

		var parsed blackbaud.AdvancedList
		if err := json.Unmarshal(body, &parsed); err != nil {
			slog.Error("JSON unmarshal failed:", slog.Any("error", err))
			continue
		}

		if len(parsed.Results.Rows) == 0 {
			break // No more data
		}

		slog.Info("Inserting data from page", slog.Int("page", page))

		for _, row := range parsed.Results.Rows {
			columns := []string{}
			values := []any{}
			for _, col := range row.Columns {
				columns = append(columns, col.Name)
				values = append(values, col.Value)
			}
			err := insert(columns, values)
			if err != nil {
				slog.Error("Unable to insert transcript info", slog.Any("error", err))
				continue
			}
		}
		err = resp.Body.Close()
		if err != nil {
			slog.Error("Unable to close response body", slog.Any("error", err))
			continue
		}
	}
}
