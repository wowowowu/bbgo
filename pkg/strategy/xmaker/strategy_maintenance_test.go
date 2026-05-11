package xmaker

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/c9s/bbgo/pkg/bbgo"
	. "github.com/c9s/bbgo/pkg/testing/testhelper"
	"github.com/c9s/bbgo/pkg/types"
)

func TestStrategy_updateQuote_MaintenanceSkip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	symbol := "BTCUSDT"
	market := Market(symbol)

	hedgeSession := &bbgo.ExchangeSession{}
	s := &Strategy{
		Symbol:            symbol,
		hedgeSession:      hedgeSession,
		makerSession:      hedgeSession,
		makerMarket:       market,
		logger:            logrus.New(),
		activeMakerOrders: bbgo.NewActiveOrderBook(symbol),
		cancelOrderDurationMetrics: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "test_cancel_order_duration",
		}),
	}

	dummyConn := types.NewConnectivity()
	dummyStream := &types.StandardStream{}
	dummyConn.Bind(dummyStream)
	dummyStream.EmitConnect()

	hedgeSession.Connectivity = types.NewConnectivityGroup(dummyConn)

	now := time.Now()
	past := types.LooseFormatTime(now.Add(-10 * time.Minute))
	future := types.LooseFormatTime(now.Add(10 * time.Minute))
	hedgeSession.Maintenance = &bbgo.MaintenanceConfig{
		StartTime: &past,
		EndTime:   &future,
	}

	err := s.updateQuote(ctx)
	assert.NoError(t, err)
}
