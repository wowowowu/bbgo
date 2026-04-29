//go:build !dnum

package xmaker

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	. "github.com/c9s/bbgo/pkg/testing/testhelper"
	"github.com/c9s/bbgo/pkg/tradeid"
	"github.com/c9s/bbgo/pkg/types"
)

func TestSplitHedge_HedgeWithBestPriceAlgo(t *testing.T) {
	tradeid.GlobalGenerator = tradeid.NewDeterministicGenerator()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	market := Market("BTCUSDT")

	// create two mocked hedge market sessions
	session1, md1, _ := newMockSession(mockCtrl, ctx, market.Symbol)
	session2, md2, _ := newMockSession(mockCtrl, ctx, market.Symbol)

	depth := Number(100.0)
	hm1 := NewHedgeMarket(&HedgeMarketConfig{
		SymbolSelector: market.Symbol,
		QuotingDepth:   depth,
	}, session1, market)
	assert.NoError(t, hm1.stream.Connect(ctx))

	hm2 := NewHedgeMarket(&HedgeMarketConfig{
		SymbolSelector: market.Symbol,
		QuotingDepth:   depth,
	}, session2, market)
	assert.NoError(t, hm2.stream.Connect(ctx))

	// hm1: better price for selling (higher bid)
	md1.EmitBookSnapshot(types.SliceOrderBook{
		Symbol: market.Symbol,
		Bids:   types.PriceVolumeSlice{{Price: Number(10005), Volume: Number(100)}},
		Asks:   types.PriceVolumeSlice{{Price: Number(10010), Volume: Number(100)}},
	})

	// hm2: worse price for selling (lower bid)
	md2.EmitBookSnapshot(types.SliceOrderBook{
		Symbol: market.Symbol,
		Bids:   types.PriceVolumeSlice{{Price: Number(10000), Volume: Number(100)}},
		Asks:   types.PriceVolumeSlice{{Price: Number(10010), Volume: Number(100)}},
	})

	strategy := &Strategy{}
	strategy.positionExposure = NewPositionExposure(market.Symbol)
	strategy.makerMarket = market
	strategy.logger = logrus.New()

	split := &SplitHedge{
		Enabled: true,
		BestPriceHedge: &BestPriceHedge{
			Enabled:     true,
			BelowAmount: Number(100000), // high enough for this test
		},
		hedgeMarketInstances: map[string]*HedgeMarket{
			"m1": hm1,
			"m2": hm2,
		},
		strategy: strategy,
		logger:   logrus.New(),
	}

	// uncovered +2 means we need hedgeDelta -2 (sell 2)
	uncovered := Number(2.0)
	hedgeDelta := uncovered.Neg()

	handled, err := split.hedgeWithBestPriceAlgo(ctx, uncovered, hedgeDelta)
	assert.NoError(t, err)
	assert.True(t, handled)

	// Strategy pending exposure should be covered by +2
	assert.Equal(t, Number(2.0), strategy.positionExposure.pending.Get())

	// hm1 should have received the cover delta because it has the best bid (10005 > 10000)
	select {
	case d := <-hm1.positionDeltaC:
		assert.Equal(t, Number(2.0), d, "hm1 should receive full cover delta")
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout waiting for hm1 position delta")
	}

	// hm2 should not have received any delta
	select {
	case <-hm2.positionDeltaC:
		t.Fatalf("hm2 should not receive any delta")
	default:
	}

	// Test fallback: position value exceeds BelowAmount
	strategy.positionExposure = NewPositionExposure(market.Symbol)
	split.BestPriceHedge.BelowAmount = Number(100) // very low

	handled, err = split.hedgeWithBestPriceAlgo(ctx, uncovered, hedgeDelta)
	assert.NoError(t, err)
	assert.False(t, handled, "should not be handled when position value is too high")
}
