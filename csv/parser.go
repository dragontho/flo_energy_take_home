package csv

import (
	"encoding/csv"
	"flo_energy_take_home/db/test_flo/public/model"
	"fmt"
	"io"
	"strconv"
	"time"
)

type NEM12Record struct {
	RecordIndicator string
	NMI             string
	IntervalLength  int
	Date            string
	Values          []float64
}

func ProcessNEM12File(reader io.Reader) ([]model.MeterReadings, error) {
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields

	var readings []model.MeterReadings
	var currentNMI string
	var currentIntervalLength int

	for {
		record, err := csvReader.Read()
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
			// Taking into account only mandatory fields
			if len(record) < 6 {
				return nil, fmt.Errorf("invalid 200 record: not enough fields")
			}
			currentNMI = record[1]
			intervalLength, err := strconv.Atoi(record[8])
			if err != nil {
				return nil, fmt.Errorf("invalid interval length: %v", err)
			}
			if !(intervalLength == 5 || intervalLength == 15 || intervalLength == 30) {
				return nil, fmt.Errorf("invalid interval length, must be one of 5, 15 or 30")
			}
			currentIntervalLength = intervalLength

		case "300":
			if len(record) < 3 {
				return nil, fmt.Errorf("invalid 300 record: not enough fields")
			}
			date, err := time.Parse("20060102", record[1])
			if err != nil {
				return nil, fmt.Errorf("invalid date %s: %v", record[1], err)
			}
			// Timestamp for consumption for past `IntervalLength` minutes
			date = date.Add(time.Duration(currentIntervalLength) * time.Minute)
			// Assuming currentIntervalLength is validated properly from the 200 record
			numberOfIntervals := 1440 / currentIntervalLength

			// Check for range of possible 300 record lengths
			if len(record) < numberOfIntervals+3 || len(record) > numberOfIntervals+7 {
				return nil, fmt.Errorf("invalid number of intervals: %d", numberOfIntervals)
			}

			for i, v := range record[2 : 2+numberOfIntervals] {
				if v == "" {
					continue
				}
				value, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid consumption value: %v", err)
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
