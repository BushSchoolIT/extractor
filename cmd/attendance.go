package cmd

import (
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"slices"
	"time"

	"github.com/BushSchoolIT/extractor/blackbaud"
	"github.com/BushSchoolIT/extractor/database"
	"github.com/spf13/cobra"
)

func Attendance(cmd *cobra.Command, args []string) {
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

	t := blackbaud.UnorderedTable{}
	for _, id := range config.Attendance.LevelIDs {
		req, err := api.NewRequest(http.MethodGet, blackbaud.ATTENDANCE_API, nil /* body */)
		q := req.URL.Query()
		q.Add("level_id", id)
		q.Add("day", time.Now().Format("01/02/2006")) // e.g., 07/12/2025
		q.Add("offering_type", "1")
		req.URL.RawQuery = q.Encode()
		resp, err := api.Client.Do(req)
		if err != nil {
			slog.Error("Unable to get attendance data", slog.String("id", id))
			continue
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			slog.Error("Unable to get read response body", slog.String("id", id))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			slog.Error("Response returned unexpected status code", slog.String("id", id), slog.Int("code", resp.StatusCode), slog.String("body", string(body)))
			continue
		}
		parsed := blackbaud.Attendance{}
		err = json.Unmarshal(body, &parsed)
		if err != nil {
			slog.Error("Unable to unmarshal attendance data", slog.String("id", id))
			continue
		}
		for _, row := range parsed.Value {
			if len(t.Columns) == 0 {
				t.Columns = slices.Collect(maps.Keys(row))
			}

			newRow := []any{}
			for _, col := range t.Columns {
				newRow = append(newRow, row[col])
			}
			t.Rows = append(t.Rows, newRow)
		}
	}
	err = db.InsertAttendance(t)
	if err != nil {
		slog.Error("Unable to insert emails", slog.Any("error", err))
		os.Exit(1)
	}
}
