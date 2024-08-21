package sql

import (
	"flo_energy_take_home/db/test_flo/public/model"
	"flo_energy_take_home/db/test_flo/public/table"
	"fmt"
	"strings"
	"sync"
	"time"
)

const defaultBatchSize = 10000

func GenerateInsertStatements(readings []model.MeterReadings, batchSize int) ([]string, error) {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	numBatches := (len(readings) + batchSize - 1) / batchSize
	results := make([]string, numBatches)
	var wg sync.WaitGroup
	errChan := make(chan error, numBatches)

	for i := 0; i < numBatches; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(readings) {
			end = len(readings)
		}

		wg.Add(1)
		go func(i int, batch []model.MeterReadings) {
			defer wg.Done()
			sql, err := generateBatchInsertStatement(batch)
			if err != nil {
				errChan <- err
				return
			}
			results[i] = sql
		}(i, readings[start:end])
	}

	wg.Wait()
	close(errChan)

	if len(errChan) > 0 {
		return nil, <-errChan // Return the first error encountered
	}

	return results, nil
}

func generateBatchInsertStatement(batch []model.MeterReadings) (string, error) {
	stmt := table.MeterReadings.INSERT(
		table.MeterReadings.Nmi,
		table.MeterReadings.Timestamp,
		table.MeterReadings.Consumption,
	).MODELS(batch)

	onConflict := stmt.ON_CONFLICT(
		table.MeterReadings.Nmi,
		table.MeterReadings.Timestamp,
	).DO_NOTHING()

	sql, args := onConflict.Sql()

	// Replace placeholders with actual values
	for i, arg := range args {
		placeholder := fmt.Sprintf("$%d", i+1)
		value, err := formatValue(arg)
		if err != nil {
			return "", fmt.Errorf("error formatting value at index %d: %v", i, err)
		}
		sql = strings.Replace(sql, placeholder, value, 1)
	}

	return sql, nil
}

func formatValue(v interface{}) (string, error) {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''")), nil
	case time.Time:
		return fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05")), nil
	case float64:
		return fmt.Sprintf("%f", val), nil
	default:
		return "", fmt.Errorf("unsupported type for argument")
	}
}
