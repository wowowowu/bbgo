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

func (m *mockFuturesService) QueryPositionRisk(ctx context.Context, symbol ...string) ([]types.PositionRisk, error) {
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

	round := NewArbitrageRound(
		fundingRate,
		types.ExchangeBinance, types.ExchangeBinance,
		minHoldingIntervals, fundingIntervalHours, spotWorker, futuresWorker, mockService)
	return round, mockService
}

func TestArbitrageRound_NumHoldingIntervals(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Funding at 08:00 UTC, 8h interval → FundingIntervalStart = 00:00 UTC
	nextFundingTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)

	t.Run("returns_zero_when_start_time_is_zero", func(t *testing.T) {
		round, _ := newTestArbitrageRound(t, ctrl, 8, 3, nextFundingTime)
		// StartTime is zero by default (round not started)
		result := round.NumHoldingIntervals(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
		assert.Equal(t, 0, result)
	})

	t.Run("returns_zero_when_current_time_before_funding_interval_start", func(t *testing.T) {
		round, _ := newTestArbitrageRound(t, ctrl, 8, 3, nextFundingTime)
		round.syncState.StartTime = time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC)
		// currentTime is before FundingIntervalStart (00:00 UTC)
		result := round.NumHoldingIntervals(time.Date(2023, 12, 31, 23, 0, 0, 0, time.UTC))
		assert.Equal(t, 0, result)
	})

	t.Run("returns_zero_within_first_interval", func(t *testing.T) {
		round, _ := newTestArbitrageRound(t, ctrl, 8, 3, nextFundingTime)
		round.syncState.StartTime = time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC)
		// 4 hours after FundingIntervalStart → still in first 8h interval
		result := round.NumHoldingIntervals(time.Date(2024, 1, 1, 4, 0, 0, 0, time.UTC))
		assert.Equal(t, 0, result)
	})

	t.Run("returns_one_after_first_interval", func(t *testing.T) {
		round, _ := newTestArbitrageRound(t, ctrl, 8, 3, nextFundingTime)
		round.syncState.StartTime = time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC)
		// exactly 8 hours after FundingIntervalStart
		result := round.NumHoldingIntervals(time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC))
		assert.Equal(t, 1, result)
	})

	t.Run("returns_three_after_24_hours_with_8h_interval", func(t *testing.T) {
		round, _ := newTestArbitrageRound(t, ctrl, 8, 3, nextFundingTime)
		round.syncState.StartTime = time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC)
		// 24 hours after FundingIntervalStart → 3 intervals
		result := round.NumHoldingIntervals(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
		assert.Equal(t, 3, result)
	})

	t.Run("rounds_down_partial_intervals", func(t *testing.T) {
		round, _ := newTestArbitrageRound(t, ctrl, 8, 3, nextFundingTime)
		round.syncState.StartTime = time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC)
		// 20 hours after FundingIntervalStart → 2.5 intervals → rounds down to 2
		result := round.NumHoldingIntervals(time.Date(2024, 1, 1, 20, 0, 0, 0, time.UTC))
		assert.Equal(t, 2, result)
	})

	t.Run("works_with_4h_funding_interval", func(t *testing.T) {
		// Funding at 04:00 UTC, 4h interval → FundingIntervalStart = 00:00 UTC
		nextFunding4h := time.Date(2024, 1, 1, 4, 0, 0, 0, time.UTC)
		round, _ := newTestArbitrageRound(t, ctrl, 4, 3, nextFunding4h)
		round.syncState.StartTime = time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC)
		// 12 hours after FundingIntervalStart → 3 intervals of 4h
		result := round.NumHoldingIntervals(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))
		assert.Equal(t, 3, result)
	})

	t.Run("works_with_non_epoch_aligned_funding_start", func(t *testing.T) {
		// Funding at 10:00 UTC, 8h interval → FundingIntervalStart = 02:00 UTC
		nextFundingOffset := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
		round, _ := newTestArbitrageRound(t, ctrl, 8, 3, nextFundingOffset)
		round.syncState.StartTime = time.Date(2024, 1, 1, 2, 30, 0, 0, time.UTC)
		// FundingIntervalStart is 02:00; currentTime 18:00 → 16h elapsed → 2 intervals
		result := round.NumHoldingIntervals(time.Date(2024, 1, 1, 18, 0, 0, 0, time.UTC))
		assert.Equal(t, 2, result)
	})
}

func TestArbitrageRound_TotalFundingIncome(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	nextFundingTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	round, mockService := newTestArbitrageRound(t, ctrl, 8, 3, nextFundingTime)

	t.Run("returns zero when startTime is zero", func(t *testing.T) {
		ctx := context.Background()
		currentTime := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
		round.SyncFundingFeeRecords(ctx, currentTime)
		result := round.TotalFundingIncome()
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
		round.SyncFundingFeeRecords(ctx, currentTime)
		result := round.TotalFundingIncome()

		expected := Number(0.005).Add(Number(0.003))
		assert.Equal(t, expected, result)
	})

	t.Run("deduplicates by transaction ID", func(t *testing.T) {
		// Call again with same income history - should not double-count
		ctx := context.Background()
		currentTime := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
		round.SyncFundingFeeRecords(ctx, currentTime)
		result := round.TotalFundingIncome()

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
		round.SyncFundingFeeRecords(ctx, currentTime)
		result := round.TotalFundingIncome()

		expected := Number(0.005).Add(Number(0.003)).Add(Number(0.002))
		assert.Equal(t, expected, result)
	})
}
