package sql

import (
	"flo_energy_take_home/db/test_flo/public/model"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestGenerateInsertStatements(t *testing.T) {
	tests := []struct {
		name           string
		readings       []model.MeterReadings
		batchSize      int
		expectedLen    int
		expectError    bool
		errorSubstring string
	}{
		{
			name: "Happy path - single batch",
			readings: []model.MeterReadings{
				{Nmi: "NMI1", Timestamp: time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC), Consumption: 10.5},
				{Nmi: "NMI2", Timestamp: time.Date(2023, 5, 1, 1, 0, 0, 0, time.UTC), Consumption: 11.5},
			},
			batchSize:   10,
			expectedLen: 1,
			expectError: false,
		},
		{
			name: "Happy path - multiple batches",
			readings: []model.MeterReadings{
				{Nmi: "NMI1", Timestamp: time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC), Consumption: 10.5},
				{Nmi: "NMI2", Timestamp: time.Date(2023, 5, 1, 1, 0, 0, 0, time.UTC), Consumption: 11.5},
				{Nmi: "NMI3", Timestamp: time.Date(2023, 5, 1, 2, 0, 0, 0, time.UTC), Consumption: 12.5},
			},
			batchSize:   2,
			expectedLen: 2,
			expectError: false,
		},
		{
			name:        "Empty readings",
			readings:    []model.MeterReadings{},
			batchSize:   10,
			expectedLen: 0,
			expectError: false,
		},
		{
			name: "Invalid batch size",
			readings: []model.MeterReadings{
				{Nmi: "NMI1", Timestamp: time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC), Consumption: 10.5},
			},
			batchSize:   0, // Should use default batch size
			expectedLen: 1,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := GenerateInsertStatements(tt.readings, tt.batchSize)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected an error, but got none")
				} else if !strings.Contains(err.Error(), tt.errorSubstring) {
					t.Errorf("Expected error containing '%s', but got: %v", tt.errorSubstring, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				if len(results) != tt.expectedLen {
					t.Errorf("Expected %d results, but got %d", tt.expectedLen, len(results))
				}

				for _, sql := range results {
					if !strings.Contains(sql, "INSERT INTO public.meter_readings") {
						t.Errorf("SQL doesn't contain expected INSERT statement: %s", sql)
					}
					if !strings.Contains(sql, "ON CONFLICT") {
						t.Errorf("SQL doesn't contain expected ON CONFLICT clause: %s", sql)
					}
				}
			}
		})
	}
}

func TestGenerateBatchInsertStatement(t *testing.T) {
	tests := []struct {
		name           string
		batch          []model.MeterReadings
		expectError    bool
		errorSubstring string
	}{
		{
			name: "Happy path",
			batch: []model.MeterReadings{
				{Nmi: "NMI1", Timestamp: time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC), Consumption: 10.5},
				{Nmi: "NMI2", Timestamp: time.Date(2023, 5, 1, 1, 0, 0, 0, time.UTC), Consumption: 11.5},
			},
			expectError: false,
		},
		{
			name:        "Empty batch",
			batch:       []model.MeterReadings{},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, err := generateBatchInsertStatement(tt.batch)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected an error, but got none")
				} else if !strings.Contains(err.Error(), tt.errorSubstring) {
					t.Errorf("Expected error containing '%s', but got: %v", tt.errorSubstring, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				if !strings.Contains(sql, "INSERT INTO meter_readings") {
					t.Errorf("SQL doesn't contain expected INSERT statement: %s", sql)
				}
				if !strings.Contains(sql, "ON CONFLICT") {
					t.Errorf("SQL doesn't contain expected ON CONFLICT clause: %s", sql)
				}

				for _, reading := range tt.batch {
					if !strings.Contains(sql, reading.Nmi) {
						t.Errorf("SQL doesn't contain expected NMI: %s", reading.Nmi)
					}
					if !strings.Contains(sql, reading.Timestamp.Format("2006-01-02 15:04:05")) {
						t.Errorf("SQL doesn't contain expected timestamp: %s", reading.Timestamp.Format("2006-01-02 15:04:05"))
					}
					if !strings.Contains(sql, fmt.Sprintf("%f", reading.Consumption)) {
						t.Errorf("SQL doesn't contain expected consumption: %f", reading.Consumption)
					}
				}
			}
		})
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		name        string
		input       interface{}
		expected    string
		expectError bool
	}{
		{
			name:        "String",
			input:       "test",
			expected:    "'test'",
			expectError: false,
		},
		{
			name:        "String with single quote",
			input:       "test's",
			expected:    "'test''s'",
			expectError: false,
		},
		{
			name:        "Time",
			input:       time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC),
			expected:    "'2023-05-01 00:00:00'",
			expectError: false,
		},
		{
			name:        "Float",
			input:       10.5,
			expected:    "10.500000",
			expectError: false,
		},
		{
			name:        "Unsupported type",
			input:       []int{1, 2, 3},
			expected:    "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := formatValue(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected an error, but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("Expected %s, but got %s", tt.expected, result)
				}
			}
		})
	}
}
