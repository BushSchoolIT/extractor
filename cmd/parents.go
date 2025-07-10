package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/BushSchoolIT/extractor/blackbaud"
	"github.com/BushSchoolIT/extractor/database"
	"github.com/spf13/cobra"
)

func Parents(cmd *cobra.Command, args []string) {
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

	for page := 1; ; page++ {
		req, err := api.NewRequest(http.MethodGet, blackbaud.AdvancedListApi(config.ParentsID, page), nil)
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
			parent := map[string]string{}
			grades := []int{}

			for _, col := range row.Columns {
				if col.Value == nil {
					continue
				}
				val := fmt.Sprintf("%v", col.Value)
				// slog.Info("thing happened", slog.Any("name", col.Name))
				switch {
				case col.Name == "email":
					parent["email"] = val
				case col.Name == "first_name":
					parent["first_name"] = val
				case col.Name == "last_name":
					parent["last_name"] = val
				// different casing because blackbaud is weird
				case strings.HasPrefix(col.Name, "Grad"):
					if gradYear, err := strconv.Atoi(val); err == nil {
						if grade := gradYearToGrade(gradYear, api.EndYear); grade >= 0 && grade <= 12 {
							grades = append(grades, grade)
						}
					}
				}
			}

			if email := strings.TrimSpace(parent["email"]); email != "" && len(grades) > 0 {
				if err := db.InsertEmail(email, parent["first_name"], parent["last_name"], grades); err != nil {
					slog.Error("Unable to add email", slog.Any("error", err))
				}
			}
		}
		err = resp.Body.Close()
		if err != nil {
			slog.Error("Unable to close response body", slog.Any("error", err))
			continue
		}
	}
}

func gradYearToGrade(graduationYear int, currentYear int) int {
	return 12 - (graduationYear - currentYear)
}
