package main

import (
	"log/slog"
	"os"

	"github.com/BushSchoolIT/extractor/cmd"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	cmd.Execute()
}
