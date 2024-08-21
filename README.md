# Flo Energy Take Home Test

This project is a take-home test for Flo Energy. It processes a CSV file and generates SQL statements based on the problem statement requirements.

## Features

- Reads data from a provided CSV file
- Generates SQL statements according to specified requirements
- Supports optional batch processing

## Prerequisites

This project was developed using Go version 1.23. It's recommended to use this version for optimal compatibility.

## Installation

1. Install the Jet SQL builder:
   ```
   go install github.com/go-jet/jet/v2/cmd/jet@latest
   ```

2. Generate the necessary files:
   ```
   jet -dsn=postgresql://<user>:<password>@localhost:5432/jetdb?sslmode=disable -schema=<schema_name> -path=./<output_dir>
   ```

3. Install dependencies:
   ```
   go mod tidy
   ```

## Usage

Run the program with a CSV file:

```
go run main.go --file=example.csv
```

To specify a batch size:

```
go run main.go --file=example.csv --batch=10
```

The output will be in the `/out` directory in the root directory.

## Development

This project is written in Go. Make sure you have Go installed on your system. The recommended version is 1.23.

## License

No specific license information provided.

## Contributing

No specific contributing guidelines provided.
