package sql

import (
	"flo_energy_take_home/db/test_flo/public/model"
	"flo_energy_take_home/db/test_flo/public/table"
	"fmt"
	"strings"
	"time"
)

func GenerateInsertStatements(readings []model.MeterReadings) (string, error) {
	stmt := table.MeterReadings.INSERT(
		table.MeterReadings.Nmi,
		table.MeterReadings.Timestamp,
		table.MeterReadings.Consumption,
	).MODELS(readings)

	// Add ON CONFLICT clause
	onConflict := stmt.ON_CONFLICT(
		table.MeterReadings.Nmi,
		table.MeterReadings.Timestamp,
	).DO_NOTHING()

	sql, args := onConflict.Sql()

	// Replace placeholders with actual values
	for i, arg := range args {
		placeholder := fmt.Sprintf("$%d", i+1)
		var value string
		switch v := arg.(type) {
		case string:
			value = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")) // Escape single quotes
		case time.Time:
			value = fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05"))
		case float64:
			value = fmt.Sprintf("%f", v)
		default:
			return "", fmt.Errorf("unsupported type for argument %d", i+1)
		}
		sql = strings.Replace(sql, placeholder, value, 1)
	}

	return sql, nil

}
