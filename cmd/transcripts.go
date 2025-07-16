package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"sync"

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

	var (
		t     blackbaud.UnorderedTable
		tLock sync.Mutex
		wg    sync.WaitGroup
		errCh = make(chan error, len(config.TranscriptListIDs))
	)
	for _, id := range config.TranscriptListIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			slog.Info("Processing List", slog.String("id", id))
			localTable, err := processTranscriptList(api, id)

			if err != nil {
				errCh <- fmt.Errorf("id: %s, error: %v", id, err)
			}

			tLock.Lock()
			defer tLock.Unlock()
			// Set columns only once
			if len(t.Columns) == 0 && len(localTable.Columns) > 0 {
				t.Columns = localTable.Columns
			}
			t.Rows = append(t.Rows, localTable.Rows...)
			slog.Info("Processed List", slog.String("id", id))
		}(id)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		slog.Error("Unable to fetch transcript info", slog.Any("error", e))
		os.Exit(1)
	}

	slog.Info("Import Complete")
	slog.Info("Starting Transcripts Database transformations")
	err = db.TranscriptOps(t, api.StartYear, api.EndYear)
	if err != nil {
		slog.Error("Unable to complete transcript operations", slog.Any("error", err))
		os.Exit(1)
	}
	slog.Info("Finished Transcripts Database transformations")
	slog.Info("Finished All Database transformations")
}

func processTranscriptList(api *blackbaud.BBAPIConnector, id string) (blackbaud.UnorderedTable, error) {
	t := blackbaud.UnorderedTable{}
	for page := 1; ; page++ {
		parsed, err := api.GetAdvancedList(id, page)
		if err != nil {
			slog.Error("Unable to get advanced list", slog.String("id", id), slog.Int("page", page))
			return t, fmt.Errorf("Unable to get advanced list, id: %s, err: %v", id, err)
		}
		if len(parsed.Results.Rows) == 0 {
			break // No more data
		}
		if len(t.Columns) == 0 {
			t.Columns = blackbaud.GetColumns(parsed.Results.Rows[0])
		}

		slog.Info("Collecting Data From Page", slog.Int("page", page), slog.String("id", id))
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
	return t, nil
}
