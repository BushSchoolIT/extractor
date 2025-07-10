package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/BushSchoolIT/extractor/database"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "bbextract",
		Short: "bbextract is the successor to BlackBaudExtractor rewritten in Go",
	}
	transcriptCmd = &cobra.Command{
		Use:   "transcripts",
		Short: "Extracts transcript info from blackbaud and imports it into the database",
		Run:   Transcripts,
	}
	fLogFile    string
	fLogLevel   string
	fConfigFile string
	fAuthFile   string
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(transcriptCmd)
	rootCmd.PersistentFlags().StringVar(&fConfigFile, "config", "config.json", "config file containing list IDs")
	rootCmd.PersistentFlags().StringVar(&fAuthFile, "auth", "bb_auth.json", "authconfig for blackbaud")
}

type Config struct {
	Parents           string          `json:"parents"`
	TranscriptListIDs []string        `json:"transcript_list_ids"`
	Postgres          database.Config `json:"postgres"`
}

func loadConfig(configPath string) (Config, error) {
	var config Config
	f, err := os.Open(configPath)
	if err != nil {
		return config, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return config, err
	}
	return config, nil
}
