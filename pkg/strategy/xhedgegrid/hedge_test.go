package grid2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

func TestHedgeSimulator_Hedge(t *testing.T) {
	market := types.Market{
		Symbol:          "BTCUSDT",
		BaseCurrency:    "BTC",
		QuoteCurrency:   "USDT",
		PricePrecision:  2,
		VolumePrecision: 4,
	}

	simulator := NewHedgeSimulator("BTCUSDT", market)

	// Manually set price for testing
	simulator.currentPrice = fixedpoint.NewFromInt(50000)
	simulator.currentTime = time.Now()

	// Initial position should be zero
	assert.Equal(t, float64(0), simulator.Position.GetBase().Float64())
	assert.Equal(t, float64(0), simulator.ProfitStats.AccumulatedPnL.Float64())

	// Hedge 1 BTC long (delta = 1) -> sells 1 BTC at 50000
	// To hedge 1 BTC long, delta should be -1
	err := simulator.Hedge(fixedpoint.NewFromInt(1), fixedpoint.NewFromInt(-1))
	assert.NoError(t, err)

	// Hedge position should be -1 BTC
	assert.Equal(t, float64(-1), simulator.Position.GetBase().Float64())
	assert.Equal(t, float64(0), simulator.ProfitStats.AccumulatedPnL.Float64())
	assert.Equal(t, float64(1), simulator.ProfitStats.AccumulatedVolume.Float64())

	// Price moves to 40000
	simulator.currentPrice = fixedpoint.NewFromInt(40000)

	// Hedge back to 0 (delta = 1) -> buys 1 BTC at 40000
	// Profit = (Sell Price - Buy Price) * Quantity = (50000 - 40000) * 1 = 10000
	err = simulator.Hedge(fixedpoint.Zero, fixedpoint.NewFromInt(1))
	assert.NoError(t, err)
	assert.Equal(t, float64(0), simulator.Position.GetBase().Float64())
	assert.Equal(t, float64(10000), simulator.ProfitStats.AccumulatedPnL.Float64())
	assert.Equal(t, float64(2), simulator.ProfitStats.AccumulatedVolume.Float64())

	assert.True(t, simulator.PositionExposure.IsClosed())
}

func TestHedgeSimulator_SpreadAndSlippage(t *testing.T) {
	market := types.Market{
		Symbol:          "BTCUSDT",
		BaseCurrency:    "BTC",
		QuoteCurrency:   "USDT",
		PricePrecision:  2,
		VolumePrecision: 4,
	}

	simulator := NewHedgeSimulator("BTCUSDT", market)
	simulator.SetSpread(fixedpoint.NewFromInt(10))         // Spread = 10
	simulator.SetSlippage(fixedpoint.NewFromFloat(0.0001)) // Slippage = 0.01%
	simulator.currentPrice = fixedpoint.NewFromInt(50000)
	simulator.currentTime = time.Now()

	// Buy 1 BTC
	// Spread adjustment: +Spread/2 = +5
	// Slippage adjustment: +Slippage * price = 0.0001 * 50000 = +5
	// Expected Price: 50000 + 5 + 5 = 50010
	err := simulator.Hedge(fixedpoint.NewFromInt(-1), fixedpoint.NewFromInt(1)) // Buying 1 BTC
	assert.NoError(t, err)
	assert.Equal(t, float64(1), simulator.Position.GetBase().Float64())
	assert.Equal(t, float64(50010), simulator.Position.AverageCost.Float64())

	// Sell 1 BTC
	// Spread adjustment: -Spread/2 = -5
	// Slippage adjustment: -Slippage * price = 0.0001 * 50000 = -5
	// Expected Price: 50000 - 5 - 5 = 49990
	simulator.currentPrice = fixedpoint.NewFromInt(50000)
	err = simulator.Hedge(fixedpoint.Zero, fixedpoint.NewFromInt(-1)) // Selling 1 BTC
	assert.NoError(t, err)
	assert.Equal(t, float64(0), simulator.Position.GetBase().Float64())
	// PnL = (49990 - 50010) * 1 = -20
	assert.Equal(t, float64(-20), simulator.ProfitStats.AccumulatedPnL.Float64())
}

func TestHedgeSimulator_Move(t *testing.T) {
	market := types.Market{
		Symbol:        "BTCUSDT",
		BaseCurrency:  "BTC",
		QuoteCurrency: "USDT",
	}
	simulator := NewHedgeSimulator("BTCUSDT", market)

	now := time.Now()
	simulator.KLines = []types.KLine{
		{
			Symbol:    "BTCUSDT",
			Open:      fixedpoint.NewFromInt(100),
			Close:     fixedpoint.NewFromInt(105),
			StartTime: types.Time(now),
			EndTime:   types.Time(now.Add(time.Minute).Add(-time.Millisecond)),
		},
		{
			Symbol:    "BTCUSDT",
			Open:      fixedpoint.NewFromInt(106),
			Close:     fixedpoint.NewFromInt(110),
			StartTime: types.Time(now.Add(time.Minute)),
			EndTime:   types.Time(now.Add(2 * time.Minute).Add(-time.Millisecond)),
		},
	}

	// Move to start of first k-line -> should use Open price
	err := simulator.Move(now)
	assert.NoError(t, err)
	assert.Equal(t, float64(100), simulator.currentPrice.Float64(), "should use open price at start of kline")

	// Move to middle of first k-line -> should use Open price
	err = simulator.Move(now.Add(30 * time.Second))
	assert.NoError(t, err)
	assert.Equal(t, float64(100), simulator.currentPrice.Float64(), "should use open price inside kline")

	// Move to end of first k-line -> should use Close price
	// Note: Move uses timestamp.Equal(k.GetEndTime().Time())
	err = simulator.Move(now.Add(time.Minute).Add(-time.Millisecond))
	assert.NoError(t, err)
	assert.Equal(t, float64(105), simulator.currentPrice.Float64(), "should use close price at end of kline")

	// Move to start of second k-line -> should use Open price of second k-line
	err = simulator.Move(now.Add(time.Minute))
	assert.NoError(t, err)
	assert.Equal(t, float64(106), simulator.currentPrice.Float64(), "should use open price of second kline")
}

func TestHedgeSimulator_Move_Optimization(t *testing.T) {
	market := types.Market{
		Symbol:        "BTCUSDT",
		BaseCurrency:  "BTC",
		QuoteCurrency: "USDT",
	}
	simulator := NewHedgeSimulator("BTCUSDT", market)

	now := time.Now()
	// Create 1000 klines
	for i := range 1000 {
		startTime := now.Add(time.Duration(i) * time.Minute)
		simulator.KLines = append(simulator.KLines, types.KLine{
			Symbol:    "BTCUSDT",
			Open:      fixedpoint.NewFromInt(int64(i)),
			Close:     fixedpoint.NewFromInt(int64(i + 1)),
			StartTime: types.Time(startTime),
			EndTime:   types.Time(startTime.Add(time.Minute).Add(-time.Millisecond)),
		})
	}

	// Move forward
	for i := range 1000 {
		// 30 seconds after start of kline -> should be Open price (i)
		err := simulator.Move(now.Add(time.Duration(i) * time.Minute).Add(30 * time.Second))
		assert.NoError(t, err)
		assert.Equal(t, float64(i), simulator.currentPrice.Float64())
		assert.Equal(t, i, simulator.lastKLineIndex)

		// At end of kline -> should be Close price (i+1)
		err = simulator.Move(now.Add(time.Duration(i) * time.Minute).Add(time.Minute).Add(-time.Millisecond))
		assert.NoError(t, err)
		assert.Equal(t, float64(i+1), simulator.currentPrice.Float64())
	}

	// Move backward - should reset and find correctly
	err := simulator.Move(now.Add(5 * time.Minute))
	assert.NoError(t, err)
	assert.Equal(t, float64(5), simulator.currentPrice.Float64())
	assert.Equal(t, 5, simulator.lastKLineIndex)

	// Move way forward
	err = simulator.Move(now.Add(2000 * time.Minute))
	assert.NoError(t, err)
	// Should remain at last kline price if not found (current behavior)
}
