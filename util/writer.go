package util

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

func WriteToSQLFilesParallel(statements []string, outputDir string) error {
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// Clear existing statement files
	if err := clearExistingStatementFiles(outputDir); err != nil {
		return fmt.Errorf("failed to clear existing statement files: %v", err)
	}

	numWorkers := runtime.NumCPU() // Use number of CPUs as the number of workers
	workChan := make(chan int, len(statements))
	errChan := make(chan error, len(statements))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range workChan {
				fileName := filepath.Join(outputDir, fmt.Sprintf("statement_%d.sql", index+1))
				if err := os.WriteFile(fileName, []byte(statements[index]+";"), 0644); err != nil {
					errChan <- fmt.Errorf("failed to write file %s: %v", fileName, err)
					return
				}
			}
		}()
	}

	// Send work to goroutines
	for i := range statements {
		workChan <- i
	}
	close(workChan)

	// Wait for all goroutines to finish
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		if err != nil {
			return err // Return the first error encountered
		}
	}

	return nil
}

func clearExistingStatementFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), "statement_") && strings.HasSuffix(entry.Name(), ".sql") {
			fullPath := filepath.Join(dir, entry.Name())
			if err := os.Remove(fullPath); err != nil {
				return fmt.Errorf("failed to remove file %s: %v", fullPath, err)
			}
		}
	}

	return nil
}
