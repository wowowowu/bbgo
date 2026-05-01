package csvsource

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/c9s/bbgo/pkg/types"
)

func TestReadKLinesFromCSV(t *testing.T) {
	klines, err := ReadAllKLineCsv("./testdata/binance/BTCUSDT-1h-2023-11-18.csv", "", types.Interval1h)
	assert.NoError(t, err)

	assert.Len(t, klines, 25)
	assert.Equal(t, int64(1700270000), klines[0].StartTime.Unix(), "StartTime")
	assert.Equal(t, int64(1700273599), klines[0].EndTime.Unix(), "EndTime")
	assert.Equal(t, 36613.91, klines[0].Open.Float64(), "Open")
	assert.Equal(t, 36613.92, klines[0].High.Float64(), "High")
	assert.Equal(t, 36388.12, klines[0].Low.Float64(), "Low")
	assert.Equal(t, 36400.01, klines[0].Close.Float64(), "Close")
	assert.Equal(t, 1005.75727, klines[0].Volume.Float64(), "Volume")
}
