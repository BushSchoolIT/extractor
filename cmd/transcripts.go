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
	db.TranscriptCleanup(api.EndYear)

	for _, id := range config.TranscriptListIDs {
		slog.Info("Processing List", slog.String("id", id))
		processTranscriptList(id, api, &db)
	}
	slog.Info("Import Complete")

	slog.Info("Starting Database transformations")
	// transformation functions
	err = db.FixNoYearlong()
	if err != nil {
		slog.Error("Unable to fix yearlongs", slog.Any("error", err))
	} else {
		slog.Info("Fixed yearlongs")
	}

	err = db.FixNonstandardGrades()
	if err != nil {
		slog.Error("Unable to fix nonstandard grades", slog.Any("error", err))
	} else {
		slog.Info("Fixed nonstandard grades")
	}

	err = db.FixFallYearlongs(api.StartYear, api.EndYear)
	if err != nil {
		slog.Error("Unable to fix fall yearlongs", slog.Any("error", err))
	} else {
		slog.Info("Fixed fall yearlongs")
	}

	err = db.InsertMissingTranscriptCategories()
	if err != nil {
		slog.Error("Unable to insert missing transcript categories", slog.Any("error", err))
	} else {
		slog.Info("Fixed missing transcript categories")
	}
}

func processTranscriptList(id string, api *blackbaud.BBAPIConnector, db *database.State) {
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
				val := col.Value
				// cannot do this SQL, the grade_id column is constrained to *not* be nil (part of the compound primary key for the db)
				if col.Name == "grade_id" && val == nil {
					val = 999999
				}
				values = append(values, val)
			}
			err := db.InsertTranscriptInfo(columns, values)
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
