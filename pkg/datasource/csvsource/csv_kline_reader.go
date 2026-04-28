package csvsource

import (
	"encoding/csv"
	"io"

	"github.com/c9s/bbgo/pkg/types"
)

var _ KLineReader = (*CSVKLineReader)(nil)

// CSVKLineReader is a KLineReader that reads from a CSV file.
type CSVKLineReader struct {
	csv *csv.Reader

	symbol   string
	interval types.Interval
}

// NewCSVKLineReader creates a new CSVKLineReader with the default Binance decoder.
func NewCSVKLineReader(csv *csv.Reader, symbol string, interval types.Interval) *CSVKLineReader {
	return &CSVKLineReader{
		csv:      csv,
		symbol:   symbol,
		interval: interval,
	}
}

// Read reads the next KLine from the underlying CSV data.
func (r *CSVKLineReader) Read() (types.KLine, error) {
	var k types.KLine

	rec, err := r.csv.Read()
	if err != nil {
		return k, err
	}

	return parseCsvKLineRecord(rec, r.symbol, r.interval)
}

// ReadAll reads all the KLines from the underlying CSV data.
func (r *CSVKLineReader) ReadAll() ([]types.KLine, error) {
	var ks []types.KLine

	_, _ = r.Read() // skip header

	for {
		k, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		ks = append(ks, k)
	}

	return ks, nil
}
