package csvsource

import (
	"fmt"
	"strconv"
	"time"

	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

// parseCsvKLineRecord parse a CSV record into a KLine.
func parseCsvKLineRecord(record []string, symbol string, interval time.Duration) (types.KLine, error) {
	var (
		k, empty types.KLine
		err      error
	)
	if len(record) < 5 {
		return k, ErrNotEnoughColumns
	}

	ts, err := strconv.ParseFloat(record[0], 64) // check for e numbers "1.70027E+12"
	if err != nil {
		return empty, fmt.Errorf("unable to parse timestamp: %w", err)
	}

	open, err := fixedpoint.NewFromString(record[1])
	if err != nil {
		return empty, fmt.Errorf("unable to parse open price: %w", err)
	}

	high, err := fixedpoint.NewFromString(record[2])
	if err != nil {
		return empty, fmt.Errorf("unable to parse high price: %w", err)
	}

	low, err := fixedpoint.NewFromString(record[3])
	if err != nil {
		return empty, fmt.Errorf("unable to parse low price: %w", err)
	}

	closePrice, err := fixedpoint.NewFromString(record[4])
	if err != nil {
		return empty, fmt.Errorf("unable to parse close price: %w", err)
	}

	volume := fixedpoint.Zero
	if len(record) >= 6 {
		volume, err = fixedpoint.NewFromString(record[5])
		if err != nil {
			return empty, ErrInvalidVolumeFormat
		}
	}

	// ts is in milliseconds, convert to seconds and nanoseconds
	tsMs := int64(ts)

	k.Symbol = symbol
	k.StartTime = types.Time(time.UnixMilli(tsMs))
	k.EndTime = types.Time(k.StartTime.Time().Add(interval - time.Millisecond))
	k.Open = open
	k.High = high
	k.Low = low
	k.Close = closePrice
	k.Volume = volume
	k.Closed = true

	return k, nil
}
