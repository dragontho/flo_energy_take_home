package util

import (
	"flag"
	"fmt"
	"path/filepath"
)

func ValidateFile(filename *string) error {
	// Validate input
	if *filename == "" {
		flag.Usage()
		return fmt.Errorf("error: CSV file name is required")
	}

	// Check file extension
	if filepath.Ext(*filename) != ".csv" {
		return fmt.Errorf("error: File must have .csv extension")
	}

	return nil
}
