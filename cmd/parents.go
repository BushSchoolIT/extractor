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
		parsed, err := api.GetAdvancedList(config.ParentsID, page)
		if err != nil {
			slog.Error("Unable to get advanced list", slog.String("id", config.ParentsID), slog.Int("page", page))
			continue
		}

		if len(parsed.Results.Rows) == 0 {
			break // No more data
		}

		slog.Info("Inserting data from page", slog.Int("page", page))

		parents := map[string]blackbaud.Parent{}
		for _, row := range parsed.Results.Rows {
			parent := blackbaud.Parent{}
			email := ""
			for _, col := range row.Columns {
				if col.Value == nil {
					continue
				}
				val := fmt.Sprintf("%v", col.Value)
				switch {
				case col.Name == "email":
					email = val
				case col.Name == "first_name":
					parent.FirstName = val
				case col.Name == "last_name":
					parent.LastName = val
				// different casing because blackbaud is weird
				case strings.HasPrefix(col.Name, "Grad"):
					if gradYear, err := strconv.Atoi(val); err == nil {
						if grade := gradYearToGrade(gradYear, api.EndYear); grade >= 0 && grade <= 12 {
							parent.Grades = append(parent.Grades, grade)
						}
					}
				}
			}

			if email := strings.TrimSpace(email); email != "" && len(parent.Grades) > 0 {
				parents[email] = parent
			}
		}
		db.InsertEmails(parents)
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
