package main

import (
	"flag"
	"flo_energy_take_home/csv"
	"flo_energy_take_home/sql"
	"flo_energy_take_home/util"
	"fmt"
	"os"
	"time"
)

func main() {
	start := time.Now()
	filename := flag.String("file", "", "CSV file to read")
	batchSize := flag.Int("batch", 10000, "Number of sql files to produce")
	//_ = flag.String("delimiter", ",", "CSV delimiter")

	flag.Parse()

	err := util.ValidateFile(filename)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	file, err := openFile(filename)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()

	readings, err := csv.ParallelProcessNEM12File(file)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	statements, _ := sql.GenerateInsertStatements(readings, *batchSize)

	err = util.WriteToSQLFilesParallel(statements, "./out")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("%.2fs elapsed\n", time.Since(start).Seconds())
}

func openFile(filename *string) (*os.File, error) {
	// Open file
	file, err := os.Open(*filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v\n", err)
	}

	// Get file info
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("error getting file info: %v\n", err)
	}

	// Print file size
	fmt.Printf("File size: %vB\n", info.Size())

	return file, nil
}
