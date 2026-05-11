package xalign

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
	"github.com/c9s/bbgo/pkg/types/mocks"
)

func TestStrategy_align_MaintenanceSkip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := &Strategy{
		PreferredSessions: []string{"binance"},
		ExpectedBalances: map[string]fixedpoint.Value{
			"BTC": fixedpoint.NewFromFloat(1.0),
		},
	}

	// Setup mock session
	mExchange := mocks.NewMockExchange(ctrl)
	mExchange.EXPECT().Name().Return(types.ExchangeName("binance")).AnyTimes()
	mExchange.EXPECT().CancelOrders(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mExchange.EXPECT().QueryAccount(gomock.Any()).Return(&types.Account{}, nil).AnyTimes()
	mStream := mocks.NewMockStream(ctrl)
	mStream.EXPECT().SetPublicOnly().AnyTimes()
	mStream.EXPECT().OnConnect(gomock.Any()).AnyTimes()
	mStream.EXPECT().OnDisconnect(gomock.Any()).AnyTimes()
	mStream.EXPECT().OnAuth(gomock.Any()).AnyTimes()
	mExchange.EXPECT().NewStream().Return(mStream).AnyTimes()
	session := bbgo.NewExchangeSession("binance", mExchange)

	// Set maintenance mode with BalanceQueryAvailable = false
	now := time.Now()
	startTime := types.LooseFormatTime(now.Add(-1 * time.Hour))
	endTime := types.LooseFormatTime(now.Add(1 * time.Hour))
	session.Maintenance = &bbgo.MaintenanceConfig{
		StartTime:             &startTime,
		EndTime:               &endTime,
		BalanceQueryAvailable: false,
	}

	sessions := bbgo.ExchangeSessionMap{
		"binance": session,
	}
	s.sessions = sessions
	s.orderBooks = map[string]*bbgo.ActiveOrderBook{
		"binance": bbgo.NewActiveOrderBook(""),
	}

	exists := s.align(ctx, sessions)
	assert.False(t, exists)
}

func TestStrategy_selectSessionForCurrency_MaintenanceSkip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := &Strategy{
		PreferredSessions: []string{"binance", "okx"},
		PreferredQuoteCurrencies: &QuoteCurrencyPreference{
			Buy:  []string{"USDT"},
			Sell: []string{"USDT"},
		},
	}

	mExchangeBinance := mocks.NewMockExchange(ctrl)
	mExchangeBinance.EXPECT().Name().Return(types.ExchangeName("binance")).AnyTimes()
	mStreamBinance := mocks.NewMockStream(ctrl)
	mStreamBinance.EXPECT().SetPublicOnly().AnyTimes()
	mStreamBinance.EXPECT().OnConnect(gomock.Any()).AnyTimes()
	mStreamBinance.EXPECT().OnDisconnect(gomock.Any()).AnyTimes()
	mStreamBinance.EXPECT().OnAuth(gomock.Any()).AnyTimes()
	mExchangeBinance.EXPECT().NewStream().Return(mStreamBinance).AnyTimes()
	sessionBinance := bbgo.NewExchangeSession("binance", mExchangeBinance)

	// Set binance in maintenance
	now := time.Now()
	startTime := types.LooseFormatTime(now.Add(-1 * time.Hour))
	endTime := types.LooseFormatTime(now.Add(1 * time.Hour))
	sessionBinance.Maintenance = &bbgo.MaintenanceConfig{
		StartTime: &startTime,
		EndTime:   &endTime,
	}

	mExchangeOkx := mocks.NewMockExchange(ctrl)
	mExchangeOkx.EXPECT().Name().Return(types.ExchangeName("okx")).AnyTimes()
	mExchangeOkx.EXPECT().QueryAccount(gomock.Any()).Return(&types.Account{}, nil).AnyTimes()
	mStreamOkx := mocks.NewMockStream(ctrl)
	mStreamOkx.EXPECT().SetPublicOnly().AnyTimes()
	mStreamOkx.EXPECT().OnConnect(gomock.Any()).AnyTimes()
	mStreamOkx.EXPECT().OnDisconnect(gomock.Any()).AnyTimes()
	mStreamOkx.EXPECT().OnAuth(gomock.Any()).AnyTimes()
	mExchangeOkx.EXPECT().NewStream().Return(mStreamOkx).AnyTimes()
	sessionOkx := bbgo.NewExchangeSession("okx", mExchangeOkx)
	// okx is NOT in maintenance

	sessions := map[string]*bbgo.ExchangeSession{
		"binance": sessionBinance,
		"okx":     sessionOkx,
	}

	// Test selecting session when binance is in maintenance
	// It should skip binance and try okx (but okx will fail account update or market check in this mock setup)
	selectedSession, _ := s.selectSessionForCurrency(ctx, sessions, "", "BTC", fixedpoint.NewFromFloat(1.0))

	// If it correctly skipped binance, selectedSession won't be binance.
	if selectedSession != nil {
		assert.NotEqual(t, "binance", selectedSession.Name)
	}
}
