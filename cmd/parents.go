package cmd

import (
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/BushSchoolIT/extractor/blackbaud"
	"github.com/BushSchoolIT/extractor/database"
	"github.com/spf13/cobra"
)

const YEAR_PREFIX string = "Grad Year"

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

	t := blackbaud.UnorderedTable{}
	for page := 1; ; page++ {
		parsed, err := api.GetAdvancedList(config.ParentsID, page)
		if err != nil {
			slog.Error("Unable to get advanced list", slog.String("id", config.ParentsID), slog.Int("page", page))
			continue
		}
		if len(parsed.Results.Rows) == 0 {
			break // No more data
		}
		if len(t.Columns) == 0 {
			t.Columns = getParentColumns(parsed.Results.Rows[0])
		}

		slog.Info("Inserting data from page", slog.Int("page", page))
		for _, row := range parsed.Results.Rows {
			grades := []int{}
			newRow := []any{}
			for _, col := range row.Columns {
				if strings.HasPrefix(col.Name, YEAR_PREFIX) {
					s, ok := col.Value.(string)
					if !ok {
						continue
					}
					val, err := strconv.Atoi(s)
					if err != nil {
						continue
					}
					grades = append(grades, gradYearToGrade(val, api.StartYear))
					continue
				}
				newRow = append(newRow, col.Value)
			}
			newRow = append(newRow, grades)
			t.Rows = append(t.Rows, newRow)
		}
	}
	err = db.InsertEmails(t)
	if err != nil {
		slog.Error("Unable to insert emails", slog.Any("error", err))
		os.Exit(1)
	}
}

func gradYearToGrade(graduationYear int, currentYear int) int {
	return 12 - (graduationYear - currentYear)
}

func getParentColumns(row blackbaud.Row) []string {
	columns := []string{}
	for _, col := range row.Columns {
		if strings.HasPrefix(col.Name, YEAR_PREFIX) {
			continue
		}
		columns = append(columns, col.Name)
	}
	// extra "grade" column because blackbaud doesn't support arrays in the list
	columns = append(columns, "grade")
	return columns
}
