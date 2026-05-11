package signal

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/c9s/bbgo/pkg/fixedpoint"
	indicatorv2 "github.com/c9s/bbgo/pkg/indicator/v2"
	"github.com/c9s/bbgo/pkg/types"
)

func TestOrderBookBestPriceVolumeSignal_CalculateSignal(t *testing.T) {
	ctx := context.Background()
	symbol := "BTCUSDT"
	book := types.NewStreamBook(symbol, types.ExchangeBinance)

	s := &OrderBookBestPriceVolumeSignal{
		RatioThreshold: fixedpoint.NewFromFloat(0.6),
		MinVolume:      fixedpoint.NewFromFloat(100.0), // Quote volume threshold
		Window:         3,
	}
	s.SetLogger(logrus.New())
	s.SetStreamBook(book)

	err := s.Bind(ctx, nil, symbol)
	assert.NoError(t, err)

	// Update 1: Bid Price 100, Vol 10 -> Quote 1000
	//           Ask Price 101, Vol 5  -> Quote 505
	// Max Bid Quote: 1000, Max Ask Quote: 505
	// Sum Quote: 1505, Bid Ratio: 1000/1505 = 0.66445
	// Signal: (0.66445 - 0.6) / (1.0 - 0.6) = 0.06445 / 0.4 = 0.1611
	book.Load(types.SliceOrderBook{
		Symbol: symbol,
		Bids:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(10.0)}},
		Asks:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(101.0), Volume: fixedpoint.NewFromFloat(5.0)}},
	})
	sig, err := s.CalculateSignal(ctx)
	assert.NoError(t, err)
	assert.InDelta(t, 0.1611, sig, 0.001)

	// Update 2: Bid Price 100, Vol 2  -> Quote 200
	//           Ask Price 101, Vol 20 -> Quote 2020
	// Max Bid Quote: 1000, Max Ask Quote: 2020
	// Sum Quote: 3020, Ask Ratio: 2020/3020 = 0.66887
	// Signal: -(0.66887 - 0.6) / 0.4 = -0.17218
	book.Load(types.SliceOrderBook{
		Symbol: symbol,
		Bids:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(2.0)}},
		Asks:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(101.0), Volume: fixedpoint.NewFromFloat(20.0)}},
	})
	sig, err = s.CalculateSignal(ctx)
	assert.NoError(t, err)
	assert.InDelta(t, -0.17218, sig, 0.001)
}

func TestOrderBookBestPriceVolumeSignal_MinDelta(t *testing.T) {
	ctx := context.Background()
	symbol := "BTCUSDT"
	book := types.NewStreamBook(symbol, types.ExchangeBinance)

	s := &OrderBookBestPriceVolumeSignal{
		RatioThreshold: fixedpoint.NewFromFloat(0.6),
		MinDelta:       fixedpoint.NewFromFloat(500.0), // Quote volume delta threshold
	}
	s.SetLogger(logrus.New())
	s.SetStreamBook(book)

	err := s.Bind(ctx, nil, symbol)
	assert.NoError(t, err)

	// Update 1: Bid Quote 1000, Ask Quote 800 -> Delta 200 < 500
	// Signal should be 0.0
	book.Load(types.SliceOrderBook{
		Symbol: symbol,
		Bids:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(10.0)}},
		Asks:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(8.0)}},
	})
	sig, err := s.CalculateSignal(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, sig)

	// Update 2: Bid Quote 1200, Ask Quote 600 -> Delta 600 > 500
	// Sum 1800, Bid Ratio 1200/1800 = 0.666
	// Signal (0.666 - 0.6) / 0.4 = 0.166
	book.Load(types.SliceOrderBook{
		Symbol: symbol,
		Bids:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(12.0)}},
		Asks:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(6.0)}},
	})
	sig, err = s.CalculateSignal(ctx)
	assert.NoError(t, err)
	assert.InDelta(t, 0.166, sig, 0.001)
}

func TestOrderBookBestPriceVolumeSignal_Smoothing(t *testing.T) {
	ctx := context.Background()
	symbol := "BTCUSDT"
	book := types.NewStreamBook(symbol, types.ExchangeBinance)

	s := &OrderBookBestPriceVolumeSignal{
		RatioThreshold:  fixedpoint.NewFromFloat(0.5),
		MinVolume:       fixedpoint.NewFromFloat(0.0),
		Window:          1, // Max of current
		SmoothingWindow: 2, // EWMA window 2 -> multiplier = 2/(1+2) = 0.666
		SmoothingType:   indicatorv2.SmoothingTypeEMA,
	}
	s.SetLogger(logrus.New())
	s.SetStreamBook(book)

	err := s.Bind(ctx, nil, symbol)
	assert.NoError(t, err)

	// Update 1: Bid Quote 1000, Ask Quote 1000
	// EMA Bid: 1000, EMA Ask: 1000
	book.Load(types.SliceOrderBook{
		Symbol: symbol,
		Bids:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(10.0)}},
		Asks:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(10.0)}},
	})
	sig, err := s.CalculateSignal(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, sig)

	// Update 2: Bid Quote 2000, Ask Quote 1000
	// multiplier m = 0.666
	// EMA Bid: (1-m)*1000 + m*2000 = 0.333*1000 + 0.666*2000 = 333.33 + 1333.33 = 1666.66
	// EMA Ask: (1-m)*1000 + m*1000 = 1000
	// Sum: 2666.66
	// Bid Ratio: 1666.66 / 2666.66 = 0.625
	// Signal: (0.625 - 0.5) / (1.0 - 0.5) = 0.125 / 0.5 = 0.25
	book.Load(types.SliceOrderBook{
		Symbol: symbol,
		Bids:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(20.0)}},
		Asks:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(10.0)}},
	})
	sig, err = s.CalculateSignal(ctx)
	assert.NoError(t, err)
	assert.InDelta(t, 0.25, sig, 0.01)
}

func TestOrderBookBestPriceVolumeSignal_SmoothingSMA(t *testing.T) {
	ctx := context.Background()
	symbol := "BTCUSDT"
	book := types.NewStreamBook(symbol, types.ExchangeBinance)

	s := &OrderBookBestPriceVolumeSignal{
		RatioThreshold:  fixedpoint.NewFromFloat(0.5),
		MinVolume:       fixedpoint.NewFromFloat(0.0),
		Window:          1, // Max of current
		SmoothingWindow: 2, // SMA window 2
		SmoothingType:   indicatorv2.SmoothingTypeSMA,
	}
	s.SetLogger(logrus.New())
	s.SetStreamBook(book)

	err := s.Bind(ctx, nil, symbol)
	assert.NoError(t, err)

	// Update 1: Bid Quote 1000, Ask Quote 1000
	// SMA Bid: 1000, SMA Ask: 1000
	book.Load(types.SliceOrderBook{
		Symbol: symbol,
		Bids:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(10.0)}},
		Asks:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(10.0)}},
	})
	sig, err := s.CalculateSignal(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, sig)

	// Update 2: Bid Quote 2000, Ask Quote 1000
	// SMA Bid: (1000 + 2000) / 2 = 1500
	// SMA Ask: (1000 + 1000) / 2 = 1000
	// Sum: 2500
	// Bid Ratio: 1500 / 2500 = 0.6
	// Signal: (0.6 - 0.5) / (1.0 - 0.5) = 0.1 / 0.5 = 0.2
	book.Load(types.SliceOrderBook{
		Symbol: symbol,
		Bids:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(20.0)}},
		Asks:   types.PriceVolumeSlice{{Price: fixedpoint.NewFromFloat(100.0), Volume: fixedpoint.NewFromFloat(10.0)}},
	})
	sig, err = s.CalculateSignal(ctx)
	assert.NoError(t, err)
	assert.InDelta(t, 0.2, sig, 0.01)
}
