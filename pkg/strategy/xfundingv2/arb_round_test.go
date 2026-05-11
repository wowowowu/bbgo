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
		round.syncState.StartTime = time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)

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
