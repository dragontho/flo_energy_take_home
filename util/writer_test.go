package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteToSQLFilesParallel(t *testing.T) {
	tests := []struct {
		name          string
		statements    []string
		existingFiles []string
		expectError   bool
		expectedFiles int
		setupFunc     func(string) error
		cleanupFunc   func(string) error
	}{
		{
			name: "Happy path - multiple statements",
			statements: []string{
				"INSERT INTO table1 VALUES (1, 'test1')",
				"INSERT INTO table1 VALUES (2, 'test2')",
				"INSERT INTO table1 VALUES (3, 'test3')",
			},
			expectError:   false,
			expectedFiles: 3,
		},
		{
			name:          "Empty statements slice",
			statements:    []string{},
			expectError:   false,
			expectedFiles: 0,
		},
		{
			name: "Clear existing files",
			statements: []string{
				"INSERT INTO table1 VALUES (1, 'new1')",
			},
			existingFiles: []string{
				"statement_2.sql",
				"statement_3.sql",
			},
			expectError:   false,
			expectedFiles: 1,
		},
		{
			name: "Error - permission denied",
			statements: []string{
				"INSERT INTO table1 VALUES (1, 'test1')",
			},
			expectError:   true,
			expectedFiles: 0,
			setupFunc: func(dir string) error {
				return os.Chmod(dir, 0555) // Read and execute permissions only
			},
			cleanupFunc: func(dir string) error {
				return os.Chmod(dir, 0755) // Restore permissions
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary directory for each test
			tempDir, err := ioutil.TempDir("", "sqltest")
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tempDir)

			// Create existing files if specified
			for _, filename := range tt.existingFiles {
				err := ioutil.WriteFile(filepath.Join(tempDir, filename), []byte("old content"), 0644)
				if err != nil {
					t.Fatalf("Failed to create existing file: %v", err)
				}
			}

			// Run setup function if provided
			if tt.setupFunc != nil {
				if err := tt.setupFunc(tempDir); err != nil {
					t.Fatalf("Setup failed: %v", err)
				}
			}

			// Run the function
			err = WriteToSQLFilesParallel(tt.statements, tempDir)

			// Check error expectation
			if tt.expectError && err == nil {
				t.Errorf("Expected an error, but got none")
			} else if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Check number of files created
			files, err := ioutil.ReadDir(tempDir)
			if err != nil {
				t.Fatalf("Failed to read temp dir: %v", err)
			}
			if len(files) != tt.expectedFiles {
				t.Errorf("Expected %d files, but got %d", tt.expectedFiles, len(files))
			}

			// Check file contents if no error is expected
			if !tt.expectError {
				for i, statement := range tt.statements {
					fileName := filepath.Join(tempDir, fmt.Sprintf("statement_%d.sql", i+1))
					content, err := ioutil.ReadFile(fileName)
					if err != nil {
						t.Errorf("Failed to read file %s: %v", fileName, err)
						continue
					}
					expectedContent := statement + ";"
					if string(content) != expectedContent {
						t.Errorf("File %s content mismatch. Expected: %s, Got: %s", fileName, expectedContent, string(content))
					}
				}
			}

			// Check that old files were cleared
			for _, filename := range tt.existingFiles {
				if _, err := os.Stat(filepath.Join(tempDir, filename)); !os.IsNotExist(err) {
					t.Errorf("Expected file %s to be deleted, but it still exists", filename)
				}
			}

			// Run cleanup function if provided
			if tt.cleanupFunc != nil {
				if err := tt.cleanupFunc(tempDir); err != nil {
					t.Fatalf("Cleanup failed: %v", err)
				}
			}
		})
	}
}

func TestWriteToSQLFilesParallel_ConcurrencyAndRaceConditions(t *testing.T) {
	// Generate a large number of statements to test concurrency
	statements := make([]string, 1000)
	for i := range statements {
		statements[i] = fmt.Sprintf("INSERT INTO table1 VALUES (%d, 'test%d')", i, i)
	}

	tempDir, err := ioutil.TempDir("", "sqltest_concurrency")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	err = WriteToSQLFilesParallel(statements, tempDir)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check that all files were created and contain correct content
	for i, statement := range statements {
		fileName := filepath.Join(tempDir, fmt.Sprintf("statement_%d.sql", i+1))
		content, err := ioutil.ReadFile(fileName)
		if err != nil {
			t.Errorf("Failed to read file %s: %v", fileName, err)
			continue
		}
		expectedContent := statement + ";"
		if string(content) != expectedContent {
			t.Errorf("File %s content mismatch. Expected: %s, Got: %s", fileName, expectedContent, string(content))
		}
	}
}
