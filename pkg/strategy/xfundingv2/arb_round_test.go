package xfundingv2

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/c9s/bbgo/pkg/exchange/binance/binanceapi"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"

	. "github.com/c9s/bbgo/pkg/testing/testhelper"
)

// mockFuturesService implements FuturesService for testing
type mockFuturesService struct {
	incomeHistory []binanceapi.FuturesIncome
	transferErr   error
}

func (m *mockFuturesService) QueryFuturesIncomeHistory(
	ctx context.Context, symbol string, incomeType binanceapi.FuturesIncomeType,
	startTime, endTime *time.Time,
) ([]binanceapi.FuturesIncome, error) {
	return m.incomeHistory, nil
}

func (m *mockFuturesService) TransferFuturesAccountAsset(
	ctx context.Context, asset string, amount fixedpoint.Value, direction types.TransferDirection,
) error {
	return m.transferErr
}

func (m *mockFuturesService) QueryPremiumIndex(ctx context.Context, symbol string) (*types.PremiumIndex, error) {
	return nil, nil
}

func newTestArbitrageRound(t *testing.T, ctrl *gomock.Controller, fundingIntervalHours, minHoldingIntervals int, nextFundingTime time.Time) (*ArbitrageRound, *mockFuturesService) {
	config := TWAPWorkerConfig{
		Duration:  10 * time.Minute,
		NumSlices: 5,
	}

	spotWorker, _, _, _ := newTestTWAPWorker(t, ctrl, config)
	spotWorker.SetTargetPosition(Number(1.0)) // long spot

	futuresWorker, _, _, _ := newTestTWAPWorker(t, ctrl, config)
	futuresWorker.SetTargetPosition(Number(-1.0)) // short futures

	mockService := &mockFuturesService{}

	fundingRate := &types.PremiumIndex{
		LastFundingRate: Number(0.001),
		NextFundingTime: nextFundingTime,
	}

	round := NewArbitrageRound(fundingRate, minHoldingIntervals, fundingIntervalHours, spotWorker, futuresWorker, mockService)
	return round, mockService
}

func TestArbitrageRound_IsClosable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// funding interval: 8 hours, min holding: 3 intervals
	// nextFundingTime = 2024-01-01 08:00 UTC
	// fundingIntervalStart = 2024-01-01 00:00 UTC
	nextFundingTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	round, _ := newTestArbitrageRound(t, ctrl, 8, 3, nextFundingTime)

	t.Run("not closable when startTime is zero", func(t *testing.T) {
		currentTime := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
		assert.False(t, round.IsClosable(currentTime))
	})

	// Set startTime to simulate a started round
	round.startTime = time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)

	t.Run("not closable before min holding intervals", func(t *testing.T) {
		// fundingIntervalStart = 2024-01-01 00:00 UTC
		// After 2 intervals (16h): currentTime = 2024-01-01 16:00 UTC
		// lastIntervalEnd = Truncate(16:00, 8h) = 16:00
		// numIntervalPassed = (16:00 - 00:00) / 8h = 2
		// 2 < 3 (minHoldingIntervals) -> not closable
		currentTime := time.Date(2024, 1, 1, 16, 0, 0, 0, time.UTC)
		assert.False(t, round.IsClosable(currentTime))
	})

	t.Run("closable after min holding intervals", func(t *testing.T) {
		// After 3 intervals (24h): currentTime = 2024-01-02 00:00 UTC
		// lastIntervalEnd = Truncate(00:00 on Jan 2, 8h) = 00:00 on Jan 2
		// numIntervalPassed = (24:00 - 00:00) / 8h = 3
		// 3 >= 3 -> closable
		currentTime := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
		assert.True(t, round.IsClosable(currentTime))
	})

	t.Run("closable well after min holding intervals", func(t *testing.T) {
		// After 5 intervals (40h): currentTime = 2024-01-02 16:00 UTC
		currentTime := time.Date(2024, 1, 2, 16, 0, 0, 0, time.UTC)
		assert.True(t, round.IsClosable(currentTime))
	})

	t.Run("not closable in middle of interval period", func(t *testing.T) {
		// currentTime = 2024-01-01 20:00 UTC (between 2nd and 3rd interval)
		// lastIntervalEnd = Truncate(20:00, 8h) = 16:00
		// numIntervalPassed = (16:00 - 00:00) / 8h = 2
		// 2 < 3 -> not closable
		currentTime := time.Date(2024, 1, 1, 20, 0, 0, 0, time.UTC)
		assert.False(t, round.IsClosable(currentTime))
	})
}

func TestArbitrageRound_CollectedFunding(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	nextFundingTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	round, mockService := newTestArbitrageRound(t, ctrl, 8, 3, nextFundingTime)

	t.Run("returns zero when startTime is zero", func(t *testing.T) {
		ctx := context.Background()
		currentTime := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
		result := round.CollectedFunding(ctx, currentTime)
		assert.Equal(t, fixedpoint.Zero, result)
	})

	t.Run("sums funding fee records", func(t *testing.T) {
		round.startTime = time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)

		// Simulate funding fee income returned by the service
		mockService.incomeHistory = []binanceapi.FuturesIncome{
			{
				Symbol:     "BTCUSDT",
				IncomeType: binanceapi.FuturesIncomeFundingFee,
				Income:     Number(0.005),
				Asset:      "USDT",
				TranId:     1001,
				Time:       types.NewMillisecondTimestampFromInt(time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC).UnixMilli()),
			},
			{
				Symbol:     "BTCUSDT",
				IncomeType: binanceapi.FuturesIncomeFundingFee,
				Income:     Number(0.003),
				Asset:      "USDT",
				TranId:     1002,
				Time:       types.NewMillisecondTimestampFromInt(time.Date(2024, 1, 1, 16, 0, 0, 0, time.UTC).UnixMilli()),
			},
		}

		ctx := context.Background()
		currentTime := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
		result := round.CollectedFunding(ctx, currentTime)

		expected := Number(0.005).Add(Number(0.003))
		assert.Equal(t, expected, result)
	})

	t.Run("deduplicates by transaction ID", func(t *testing.T) {
		// Call again with same income history - should not double-count
		ctx := context.Background()
		currentTime := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
		result := round.CollectedFunding(ctx, currentTime)

		expected := Number(0.005).Add(Number(0.003))
		assert.Equal(t, expected, result)
	})

	t.Run("accumulates new records", func(t *testing.T) {
		// Add a third funding fee
		mockService.incomeHistory = append(mockService.incomeHistory, binanceapi.FuturesIncome{
			Symbol:     "BTCUSDT",
			IncomeType: binanceapi.FuturesIncomeFundingFee,
			Income:     Number(0.002),
			Asset:      "USDT",
			TranId:     1003,
			Time:       types.NewMillisecondTimestampFromInt(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).UnixMilli()),
		})

		ctx := context.Background()
		currentTime := time.Date(2024, 1, 2, 8, 0, 0, 0, time.UTC)
		result := round.CollectedFunding(ctx, currentTime)

		expected := Number(0.005).Add(Number(0.003)).Add(Number(0.002))
		assert.Equal(t, expected, result)
	})
}
