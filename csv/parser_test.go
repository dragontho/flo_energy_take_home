package csv

import (
	"flo_energy_take_home/db/test_flo/public/model"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParallelProcessNEM12File(t *testing.T) {
	runTestCases(t, func(content string) ([]model.MeterReadings, error) {
		// Create a temporary file
		tmpfile, err := os.CreateTemp("", "test*.csv")
		if err != nil {
			return nil, err
		}
		defer os.Remove(tmpfile.Name()) // Clean up

		// Write content to the file
		if _, err := tmpfile.Write([]byte(content)); err != nil {
			return nil, err
		}
		if err := tmpfile.Close(); err != nil {
			return nil, err
		}
		file, err := os.Open(tmpfile.Name())
		if err != nil {
			return nil, fmt.Errorf("error opening file: %v\n", err)
		}
		defer file.Close()
		return ParallelProcessNEM12File(file)
	})
}

func runTestCases(t *testing.T, processFn func(string) ([]model.MeterReadings, error)) {
	tests := []struct {
		name          string
		input         string
		expectedLen   int
		expectedNMI   string
		expectedTime  string
		expectedValue float64
		expectError   bool
		errorMessage  string
	}{
		{
			name: "Valid 30 minute intervals",
			input: `100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204
900`,
			expectedLen:   48,
			expectedNMI:   "NEM1201009",
			expectedTime:  "2005-03-01 00:30:00",
			expectedValue: 0,
			expectError:   false,
		},
		{
			name: "Valid 15 minute intervals",
			input: `100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201010,E1E2,1,E1,N1,01010,kWh,15,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204
900`,
			expectedLen:   96,
			expectedNMI:   "NEM1201010",
			expectedTime:  "2005-03-01 00:15:00",
			expectedValue: 0,
			expectError:   false,
		},
		{
			name: "Valid 5 minute intervals",
			input: `100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201011,E1E2,1,E1,N1,01011,kWh,5,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204
900`,
			expectedLen:   288,
			expectedNMI:   "NEM1201011",
			expectedTime:  "2005-03-01 00:05:00",
			expectedValue: 0,
			expectError:   false,
		},
		{
			name: "Invalid interval length",
			input: `100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201012,E1E2,1,E1,N1,01012,kWh,10,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,A,,,20050310121004,20050310182204
900`,
			expectError:  true,
			errorMessage: "invalid interval length, must be one of 5, 15 or 30",
		},
		{
			name: "Invalid 200 record - not enough fields",
			input: `100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201013
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,A,,,20050310121004,20050310182204
900`,
			expectError:  true,
			errorMessage: "invalid 200 record: not enough fields",
		},
		{
			name: "Invalid 300 record - not enough fields",
			input: `100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201014,E1E2,1,E1,N1,01014,kWh,30,20050610
300,20050301
900`,
			expectError:  true,
			errorMessage: "invalid 300 record: not enough fields",
		},
		{
			name: "Invalid 300 record - too few intervals",
			input: `100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201015,E1E2,1,E1,N1,01015,kWh,30,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,A,,,20050310121004,20050310182204
900`,
			expectError:  true,
			errorMessage: "invalid number of intervals: 48",
		},
		{
			name: "Invalid 300 record - too many intervals",
			input: `100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201016,E1E2,1,E1,N1,01016,kWh,30,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,0.1,0.2,0.3,0.4,0.5,0.6,A,,,20050310121004,20050310182204
900`,
			expectError:  true,
			errorMessage: "invalid number of intervals: 48",
		},
		{
			name: "Invalid consumption value",
			input: `100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201017,E1E2,1,E1,N1,01017,kWh,30,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,INVALID,A,,,20050310121004,20050310182204
900`,
			expectError:  true,
			errorMessage: "invalid consumption value:",
		},
		// New test case for parallel processing
		{
			name: "Multiple NMIs for parallel processing",
			input: `100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204
200,NEM1201010,E1E2,1,E1,N1,01010,kWh,30,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204
900`,
			expectedLen:   96,
			expectedNMI:   "",
			expectedTime:  "2005-03-01 00:30:00",
			expectedValue: 0,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readings, err := processFn(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected an error but got none")
				} else if !strings.Contains(err.Error(), tt.errorMessage) {
					t.Errorf("Expected error message to contain '%s', but got: %v", tt.errorMessage, err)
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				if len(readings) != tt.expectedLen {
					t.Errorf("Expected %d readings, but got %d", tt.expectedLen, len(readings))
				}

				if len(readings) > 0 {
					firstReading := readings[0]
					expectedTime, _ := time.Parse("2006-01-02 15:04:05", tt.expectedTime)

					if tt.expectedNMI != "" && firstReading.Nmi != tt.expectedNMI {
						t.Errorf("Expected NMI %s, but got %s", tt.expectedNMI, firstReading.Nmi)
					}
					if !firstReading.Timestamp.Equal(expectedTime) {
						t.Errorf("Expected timestamp %v, but got %v", expectedTime, firstReading.Timestamp)
					}
					if firstReading.Consumption != tt.expectedValue {
						t.Errorf("Expected consumption %f, but got %f", tt.expectedValue, firstReading.Consumption)
					}
				}
			}
		})
	}
}

func TestSplitFileIntoChunks(t *testing.T) {
	content := `100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234
200,NEM1201010,E1E2,1,E1,N1,01010,kWh,30,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234
900`

	tmpfile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	file, err := os.Open(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to open temp file: %v", err)
	}
	defer file.Close()

	chunks, err := splitFileIntoChunks(file, 2)
	if err != nil {
		t.Fatalf("splitFileIntoChunks returned an error: %v", err)
	}

	fmt.Printf("Chunks: %v\n", chunks)

	if len(chunks) != 2 {
		t.Errorf("Expected 2 chunks, but got %d", len(chunks))
	}

	if !strings.HasPrefix(chunks[0][0], "100,NEM12") {
		t.Errorf("First chunk should start with 100 record, but got: %s", chunks[0][0])
	}

	if !strings.HasPrefix(chunks[1][0], "200,NEM1201009") {
		t.Errorf("Second chunk should start with 200 record for NEM1201010, but got: %s", chunks[1][0])
	}
}
