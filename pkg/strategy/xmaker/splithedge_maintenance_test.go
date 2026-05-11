package xmaker

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/c9s/bbgo/pkg/bbgo"
	. "github.com/c9s/bbgo/pkg/testing/testhelper"
	"github.com/c9s/bbgo/pkg/types"
)

func TestSplitHedge_MaintenanceSkip(t *testing.T) {
	ctx := context.Background()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	market := Market("BTCUSDT")
	now := time.Now()
	past := types.LooseFormatTime(now.Add(-time.Hour))
	future := types.LooseFormatTime(now.Add(time.Hour))

	// session1: in maintenance
	session1, md1, _ := newMockSession(mockCtrl, ctx, market.Symbol)
	session1.Maintenance = &bbgo.MaintenanceConfig{
		StartTime: &past,
		EndTime:   &future,
	}

	// session2: not in maintenance
	session2, md2, _ := newMockSession(mockCtrl, ctx, market.Symbol)

	depth := Number(100.0)
	hm1 := NewHedgeMarket(&HedgeMarketConfig{
		SymbolSelector: market.Symbol,
		HedgeInterval:  hedgeInterval,
		QuotingDepth:   depth,
	}, session1, market)
	assert.NoError(t, hm1.stream.Connect(ctx))

	hm2 := NewHedgeMarket(&HedgeMarketConfig{
		SymbolSelector: market.Symbol,
		HedgeInterval:  hedgeInterval,
		QuotingDepth:   depth,
	}, session2, market)
	assert.NoError(t, hm2.stream.Connect(ctx))

	orderBook := types.SliceOrderBook{
		Symbol: market.Symbol,
		Bids:   types.PriceVolumeSlice{{Price: Number(10000), Volume: Number(100)}},
		Asks:   types.PriceVolumeSlice{{Price: Number(10010), Volume: Number(100)}},
	}
	md1.EmitBookSnapshot(orderBook)
	md2.EmitBookSnapshot(orderBook)

	strategy := &Strategy{}
	strategy.positionExposure = bbgo.NewPositionExposure(market.Symbol)
	strategy.makerMarket = market
	strategy.logger = logrus.New()

	split := &SplitHedge{
		Enabled: true,
		Algo:    SplitHedgeAlgoProportion,
		ProportionAlgo: &SplitHedgeProportionAlgo{
			ProportionMarkets: []*SplitHedgeProportionMarket{
				{Name: "m1", Ratio: Number(0.5)},
				{Name: "m2"},
			},
		},
		hedgeMarketInstances: map[string]*HedgeMarket{
			"m1": hm1,
			"m2": hm2,
		},
		strategy: strategy,
		logger:   logrus.New(),
	}

	t.Run("hedgeWithProportionAlgo skips m1", func(t *testing.T) {
		uncovered := Number(2.0)
		hedgeDelta := uncovered.Neg()
		err := split.hedgeWithProportionAlgo(ctx, uncovered, hedgeDelta)
		assert.NoError(t, err)

		// Since m1 is skipped, m2 should take its share.
		// Wait for channel write
		time.Sleep(10 * time.Millisecond)

		select {
		case <-hm1.positionDeltaC:
			t.Errorf("hm1 should have been skipped")
		default:
		}

		select {
		case d := <-hm2.positionDeltaC:
			// RatioBase is Total by default.
			// m1 is skipped.
			// m2 is last, it takes remaining. Total was 2.0. Remaining is 2.0.
			assert.Equal(t, Number(2.0), d)
		case <-time.After(100 * time.Millisecond):
			t.Errorf("hm2 should have received delta")
		}
	})

	t.Run("GetBalanceWeightedQuotePrice skips m1", func(t *testing.T) {
		// Set some balances
		session1.GetAccount().UpdateBalances(types.BalanceMap{
			market.BaseCurrency:  {Available: Number(1.0)},
			market.QuoteCurrency: {Available: Number(10000.0)},
		})
		session2.GetAccount().UpdateBalances(types.BalanceMap{
			market.BaseCurrency:  {Available: Number(2.0)},
			market.QuoteCurrency: {Available: Number(20000.0)},
		})

		bid, ask, ok := split.GetBalanceWeightedQuotePrice()
		assert.True(t, ok)
		// Prices should come only from m2
		assert.Equal(t, Number(10000), bid)
		assert.Equal(t, Number(10010), ask)
	})

	t.Run("hedgeWithBestPriceAlgo skips m1", func(t *testing.T) {
		// Enabled best price hedge
		split.BestPriceHedge = &BestPriceHedge{
			Enabled:     true,
			BelowAmount: Number(1000000), // very high to ensure it doesn't fallback
		}

		uncovered := Number(2.0)
		hedgeDelta := uncovered.Neg()

		// session1 has better price but it's in maintenance
		// bids: session1: 10001, session2: 10000
		md1.EmitBookSnapshot(types.SliceOrderBook{
			Symbol: market.Symbol,
			Bids:   types.PriceVolumeSlice{{Price: Number(10001), Volume: Number(100)}},
			Asks:   types.PriceVolumeSlice{{Price: Number(10010), Volume: Number(100)}},
		})

		handled, err := split.hedgeWithBestPriceAlgo(ctx, uncovered, hedgeDelta)
		assert.NoError(t, err)
		assert.True(t, handled)

		// Wait for channel write
		time.Sleep(10 * time.Millisecond)

		select {
		case <-hm1.positionDeltaC:
			t.Errorf("hm1 should have been skipped")
		default:
		}

		select {
		case d := <-hm2.positionDeltaC:
			// hm2 was chosen because hm1 is in maintenance
			assert.Equal(t, Number(2.0), d)
		case <-time.After(100 * time.Millisecond):
			t.Errorf("hm2 should have received delta")
		}
	})
}
