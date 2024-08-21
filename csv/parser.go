package csv

import (
	"bufio"
	"encoding/csv"
	"flo_energy_take_home/db/test_flo/public/model"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func ParallelProcessNEM12File(file *os.File) ([]model.MeterReadings, error) {
	numWorkers := runtime.NumCPU()
	chunks, err := splitFileIntoChunks(file, numWorkers)
	if err != nil {
		return nil, fmt.Errorf("error splitting file: %v", err)
	}

	var wg sync.WaitGroup
	readingsChan := make(chan []model.MeterReadings, numWorkers)
	errorsChan := make(chan error, numWorkers)

	for _, chunk := range chunks {
		wg.Add(1)
		go func(chunk []string) {
			defer wg.Done()
			readings, err := processChunk(chunk)
			if err != nil {
				errorsChan <- err
				return
			}
			readingsChan <- readings
		}(chunk)
	}

	go func() {
		wg.Wait()
		close(readingsChan)
		close(errorsChan)
	}()

	var allReadings []model.MeterReadings
	for readings := range readingsChan {
		allReadings = append(allReadings, readings...)
	}

	if len(errorsChan) > 0 {
		return nil, <-errorsChan // Return the first error encountered
	}

	return allReadings, nil
}

func splitFileIntoChunks(file *os.File, numChunks int) ([][]string, error) {
	scanner := bufio.NewScanner(file)
	var chunks [][]string
	var currentChunk []string
	var inRecord200 bool

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ",")

		if fields[0] == "200" {
			if len(currentChunk) > 0 {
				chunks = append(chunks, currentChunk)
				currentChunk = []string{}
			}
			inRecord200 = true
		}

		currentChunk = append(currentChunk, line)

		if fields[0] == "300" && inRecord200 {
			inRecord200 = false
		}

		if len(chunks) == numChunks-1 && !inRecord200 {
			break
		}
	}

	if len(currentChunk) > 0 {
		chunks = append(chunks, currentChunk)
	}

	// Add any remaining lines to the last chunk
	for scanner.Scan() {
		chunks[len(chunks)-1] = append(chunks[len(chunks)-1], scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}

	return chunks, nil
}

func processChunk(chunk []string) ([]model.MeterReadings, error) {
	reader := csv.NewReader(strings.NewReader(strings.Join(chunk, "\n")))
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	var readings []model.MeterReadings
	var currentNMI string
	var currentIntervalLength int

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV: %v", err)
		}

		if len(record) == 0 {
			continue
		}

		switch record[0] {
		case "200":
			if len(record) < 9 {
				return nil, fmt.Errorf("invalid 200 record: not enough fields. record: %v", record)
			}
			currentNMI = record[1]
			intervalLength, err := strconv.Atoi(record[8])
			if err != nil {
				return nil, fmt.Errorf("invalid interval length: %v. record: %v", err, record)
			}
			if !(intervalLength == 5 || intervalLength == 15 || intervalLength == 30) {
				return nil, fmt.Errorf("invalid interval length, must be one of 5, 15 or 30. record: %v", record)
			}
			currentIntervalLength = intervalLength

		case "300":
			if len(record) < 3 {
				return nil, fmt.Errorf("invalid 300 record: not enough fields. record: %v", record)
			}
			date, err := time.Parse("20060102", record[1])
			if err != nil {
				return nil, fmt.Errorf("invalid date %s: %v. record: %v", record[1], err, record)
			}
			// Timestamp for consumption for past `IntervalLength` minutes
			date = date.Add(time.Duration(currentIntervalLength) * time.Minute)
			// Assuming currentIntervalLength is validated properly from the 200 record
			numberOfIntervals := 1440 / currentIntervalLength

			// Check for range of possible 300 record lengths
			if len(record) < numberOfIntervals+3 || len(record) > numberOfIntervals+7 {
				return nil, fmt.Errorf("invalid number of intervals: %d. record: %v", numberOfIntervals, record)
			}

			for i, v := range record[2 : 2+numberOfIntervals] {
				if v == "" {
					continue
				}
				value, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid consumption value: %v. record: %v", err, record)
				}
				timestamp := date.Add(time.Duration(i*currentIntervalLength) * time.Minute)
				reading := model.MeterReadings{
					Nmi:         currentNMI,
					Timestamp:   timestamp,
					Consumption: value,
				}
				readings = append(readings, reading)
			}
		}
	}

	return readings, nil
}
