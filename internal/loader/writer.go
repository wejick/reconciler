package loader

import (
	"os"

	"github.com/gocarina/gocsv"
	"github.com/wejick/reconciler/internal/model"
)

func WriteUnmatchedResults(unmatchedResults []model.UnmatchedResult, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalFile(unmatchedResults, file); err != nil {
		return err
	}

	return nil
}

func WriteSummary(summary []model.ReconciliationSummary, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalFile(summary, file); err != nil {
		return err
	}

	return nil
}
