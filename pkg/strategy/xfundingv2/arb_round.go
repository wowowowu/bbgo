package xfundingv2

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/exchange/batch"
	"github.com/c9s/bbgo/pkg/exchange/binance/binanceapi"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

//go:generate stringer -type=RoundState
type RoundState int

const (
	RoundPending RoundState = iota
	RoundOpening
	RoundReady
	RoundClosing
	RoundClosed
)

type FuturesService interface {
	batch.BinanceFuturesIncomeHistoryService

	TransferFuturesAccountAsset(ctx context.Context, asset string, amount fixedpoint.Value, io types.TransferDirection) error
	QueryPremiumIndex(ctx context.Context, symbol string) (*types.PremiumIndex, error)
}

type transferRetry struct {
	Trade     types.Trade `json:"trade"`
	LastTried time.Time   `json:"lastTried"`
}

type ArbitrageRound struct {
	mu sync.Mutex

	syncState ArbitrageRoundSyncState

	futuresService                                FuturesService
	spotExchangeFeeRates, futuresExchangeFeeRates map[types.ExchangeName]types.ExchangeFee

	retryTransferTickC chan time.Time

	logger logrus.FieldLogger
}

func NewArbitrageRound(
	fundingRate *types.PremiumIndex, minHoldingIntervals, fundingIntervalHours int,
	spotTwap, futuresTwap *TWAPWorker, futuresService FuturesService) *ArbitrageRound {
	var asset string
	if spotTwap.TargetPosition().Sign() > 0 {
		// long spot, short futures -> collateral is base asset
		asset = spotTwap.Market().BaseCurrency
	} else {
		// short spot, long futures -> collateral is quote asset
		asset = spotTwap.Market().QuoteCurrency
	}
	fundingIntervalStart := fundingRate.NextFundingTime.Add(-time.Duration(fundingIntervalHours) * time.Hour)
	fundingIntervalEnd := fundingRate.NextFundingTime.Add(-time.Second)
	return &ArbitrageRound{
		syncState: ArbitrageRoundSyncState{
			Symbol:                      spotTwap.Symbol(),
			TriggeredFundingRate:        fundingRate.LastFundingRate,
			TriggeredSpotTargetPosition: spotTwap.TargetPosition(),
			MinHoldingIntervals:         minHoldingIntervals,
			FundingIntervalHours:        fundingIntervalHours,
			FundingIntervalStart:        fundingIntervalStart,
			FundingIntervalEnd:          fundingIntervalEnd,
			FundingFeeRecords:           make(map[int64]FundingFee),

			SpotWorker:     spotTwap,
			FuturesWorker:  futuresTwap,
			Asset:          asset,
			State:          RoundPending,
			RetryTransfers: make(map[uint64]transferRetry),
		},

		futuresService:     futuresService,
		retryTransferTickC: make(chan time.Time, 1),
	}
}

func (r *ArbitrageRound) SetSpotExchangeFeeRates(rates map[types.ExchangeName]types.ExchangeFee) {
	r.spotExchangeFeeRates = rates
}

func (r *ArbitrageRound) SetFuturesExchangeFeeRates(rates map[types.ExchangeName]types.ExchangeFee) {
	r.futuresExchangeFeeRates = rates
}

func (r *ArbitrageRound) SetAvgFeeCost(feeSymbol string, cost fixedpoint.Value) {
	r.syncState.FeeSymbol = feeSymbol
	r.syncState.AvgFeeCost = cost
}

func (r *ArbitrageRound) SetSpotFeeAssetAmount(amount fixedpoint.Value) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.syncState.SpotFeeAssetAmount = amount
}

func (r *ArbitrageRound) SpotFeeAssetAmount() fixedpoint.Value {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.syncState.SpotFeeAssetAmount
}

func (r *ArbitrageRound) SetFuturesFeeAssetAmount(amount fixedpoint.Value) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.syncState.FuturesFeeAssetAmount = amount
}

func (r *ArbitrageRound) FuturesFeeAssetAmount() fixedpoint.Value {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.syncState.FuturesFeeAssetAmount
}

// RequiredFeeAssetAmount returns the required fee asset amount for the round based on its current state and position.
// The first return value is for the spot leg and the second return value is for the futures leg.
func (r *ArbitrageRound) RequiredFeeAssetAmounts() (fixedpoint.Value, fixedpoint.Value) {
	r.mu.Lock()
	defer r.mu.Unlock()

	halfSpotFee := r.syncState.SpotFeeAssetAmount.Div(fixedpoint.Two)
	halfFuturesFee := r.syncState.FuturesFeeAssetAmount.Div(fixedpoint.Two)
	switch r.syncState.State {
	case RoundPending:
		return r.syncState.SpotFeeAssetAmount, r.syncState.FuturesFeeAssetAmount
	case RoundOpening:
		// calculate the executed ratio
		executedRatio := fixedpoint.Zero
		if !r.syncState.SpotWorker.TargetPosition().IsZero() {
			executedRatio = r.syncState.SpotWorker.FilledPosition().
				Abs().
				Div(r.syncState.SpotWorker.TargetPosition().Abs())
		}
		remainRatio := fixedpoint.Max(
			fixedpoint.One.Sub(executedRatio),
			fixedpoint.Zero,
		)
		// add 1x for the closing leg fee
		remainRatio = remainRatio.Add(fixedpoint.One)
		return halfSpotFee.Mul(remainRatio), halfFuturesFee.Mul(remainRatio)
	case RoundReady, RoundClosing:
		executedRatio := fixedpoint.Zero
		if !r.syncState.TriggeredSpotTargetPosition.IsZero() {
			executedRatio = r.syncState.SpotWorker.FilledPosition().Abs().Div(r.syncState.TriggeredSpotTargetPosition.Abs())
		}
		remainRatio := fixedpoint.Max(
			fixedpoint.One.Sub(executedRatio),
			fixedpoint.Zero,
		)
		return halfSpotFee.Mul(remainRatio), halfFuturesFee.Mul(remainRatio)
	}
	// the round is closed, no fee asset is required
	return fixedpoint.Zero, fixedpoint.Zero
}

func (r *ArbitrageRound) StartTime() time.Time {
	return r.syncState.StartTime
}

func (r *ArbitrageRound) HasStarted() bool {
	return !r.syncState.StartTime.IsZero()
}

func (r *ArbitrageRound) TriggeredFundingRate() fixedpoint.Value {
	return r.syncState.TriggeredFundingRate
}

func (r *ArbitrageRound) NumHoldingIntervals(currentTime time.Time) int {
	if r.syncState.StartTime.IsZero() {
		return 0
	}
	// the funding rate has not flipped, check if the minimum holding time has passed
	intervalDuration := time.Duration(r.syncState.FundingIntervalHours) * time.Hour
	lastIntervalEnd := currentTime.Truncate(intervalDuration)
	return int(lastIntervalEnd.Sub(r.syncState.FundingIntervalStart) / intervalDuration)
}

func (r *ArbitrageRound) MinHoldingIntervals() int {
	return r.syncState.MinHoldingIntervals
}

func (r *ArbitrageRound) TargetPosition() fixedpoint.Value {
	return r.syncState.SpotWorker.TargetPosition()
}

func (r *ArbitrageRound) LastUpdateTime() time.Time {
	return r.syncState.LastUpdateTime
}

func (r *ArbitrageRound) SetUpdateTime(t time.Time) {
	r.syncState.LastUpdateTime = t
}

func (r *ArbitrageRound) String() string {
	if r.syncState.State != RoundClosing {
		return fmt.Sprintf(
			"ArbitrageRound(symbol=%s, state=%s, spot=%s, futures=%s, startTime=%s)",
			r.syncState.SpotWorker.Symbol(),
			r.syncState.State,
			r.syncState.SpotWorker.FilledPosition(),
			r.syncState.FuturesWorker.FilledPosition(),
			r.syncState.StartTime.Format(time.RFC3339),
		)
	}
	return fmt.Sprintf(
		"ArbitrageRound(symbol=%s, state=%s, spot=%s, futures=%s, closingTime=%s, expectedCloseTime=%s)",
		r.syncState.SpotWorker.Symbol(),
		r.syncState.State,
		r.syncState.SpotWorker.FilledPosition(),
		r.syncState.FuturesWorker.FilledPosition(),
		r.syncState.ClosingTime.Format(time.RFC3339),
		r.syncState.ClosingTime.Add(r.syncState.ClosingDuration).Format(time.RFC3339),
	)
}

func (r *ArbitrageRound) CollectedFunding(ctx context.Context, currentTime time.Time) fixedpoint.Value {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.syncState.StartTime.IsZero() {
		return fixedpoint.Zero
	}
	return r.collectedFunding(ctx, currentTime)
}

func (r *ArbitrageRound) collectedFunding(ctx context.Context, currentTime time.Time) fixedpoint.Value {
	r.syncFundingFeeRecords(ctx, currentTime)

	var totalFunding fixedpoint.Value
	for _, fee := range r.syncState.FundingFeeRecords {
		totalFunding = totalFunding.Add(fee.Amount)
	}
	return totalFunding
}

func (r *ArbitrageRound) SyncFundingFeeRecords(ctx context.Context, currentTime time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.syncFundingFeeRecords(ctx, currentTime)
}

func (r *ArbitrageRound) Orders() map[string][]types.Order {
	r.mu.Lock()
	defer r.mu.Unlock()

	orders := map[string][]types.Order{
		"spot":    r.syncState.SpotWorker.Executor().AllOrders(),
		"futures": r.syncState.FuturesWorker.Executor().AllOrders(),
	}

	return orders
}

func (r *ArbitrageRound) Trades() map[string][]types.Trade {
	r.mu.Lock()
	defer r.mu.Unlock()

	trades := map[string][]types.Trade{
		"spot":    r.syncState.SpotWorker.Executor().AllTrades(),
		"futures": r.syncState.FuturesWorker.Executor().AllTrades(),
	}

	return trades
}

func (r *ArbitrageRound) HasOrder(orderID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, spotExists := r.syncState.SpotWorker.Executor().GetOrder(orderID)
	_, futuresExists := r.syncState.FuturesWorker.Executor().GetOrder(orderID)

	return spotExists || futuresExists
}

func (r *ArbitrageRound) syncFundingFeeRecords(ctx context.Context, currentTime time.Time) {
	if r.syncState.StartTime.IsZero() || r.syncState.StartTime.After(currentTime) {
		return
	}

	q := batch.BinanceFuturesIncomeBatchQuery{
		BinanceFuturesIncomeHistoryService: r.futuresService,
	}
	symbol := r.syncState.FuturesWorker.Symbol()
	dataC, errC := q.Query(ctx, symbol, binanceapi.FuturesIncomeFundingFee, r.syncState.StartTime, currentTime)
	for {
		select {
		case <-ctx.Done():
			return

		case income, ok := <-dataC:
			if !ok {
				return
			}
			switch income.IncomeType {
			case binanceapi.FuturesIncomeFundingFee:
				record := FundingFee{
					Asset:  income.Asset,
					Amount: income.Income,
					Txn:    income.TranId,
					Time:   income.Time.Time(),
				}
				r.syncState.FundingFeeRecords[income.TranId] = record
			}
		case err, ok := <-errC:
			if !ok {
				return
			}

			r.logger.WithError(err).Errorf("unable to query futures income history")
			return

		}
	}
}

func (r *ArbitrageRound) Start(ctx context.Context, currentTime time.Time) error {
	if r.syncState.StartTime.IsZero() {
		if currentTime.After(r.syncState.FundingIntervalEnd) {
			// the round is triggered after the funding interval -> error
			return fmt.Errorf(
				"the round is triggered after the funding interval end (%s): %s",
				r.syncState.FundingIntervalEnd.Format(time.RFC3339),
				currentTime.Format(time.RFC3339),
			)
		}
		if err := r.syncState.SpotWorker.Start(ctx, currentTime); err != nil {
			return fmt.Errorf("failed to start spot worker: %w", err)
		}
		if err := r.syncState.FuturesWorker.Start(ctx, currentTime); err != nil {
			return fmt.Errorf("failed to start futures worker: %w", err)
		}

		go r.retryTransferWorker(ctx, r.retryTransferTickC)

		r.syncState.StartTime = currentTime
		r.syncState.State = RoundOpening
	}
	return nil
}

func (r *ArbitrageRound) Stop() {
	r.syncState.SpotWorker.Stop()
	r.syncState.FuturesWorker.Stop()
	close(r.retryTransferTickC)
}

func (r *ArbitrageRound) retryTransferWorker(ctx context.Context, tickC <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case currentTime, ok := <-tickC:
			if !ok {
				return
			}
			// retry failed transfers if any
			r.mu.Lock()
			for tradeID, transfer := range r.syncState.RetryTransfers {
				if r.syncState.RetryDuration == 0 {
					// default retry duration is 10 minutes
					r.syncState.RetryDuration = 10 * time.Minute
				}
				if currentTime.Sub(transfer.LastTried) < r.syncState.RetryDuration {
					continue
				}
				r.logger.Infof("retry transfer (trade: %d): %s %s", tradeID, transfer.Trade.Quantity.String(), r.syncState.Asset)
				r.HandleSpotTrade(transfer.Trade, currentTime)
			}
			r.mu.Unlock()
		}
	}
}

func (r *ArbitrageRound) SetRetryDuration(d time.Duration) {
	r.syncState.RetryDuration = d
}

func (r *ArbitrageRound) HandleSpotTrade(trade types.Trade, currentTime time.Time) {
	// lock the round to ensure the state is updated correctly when receiving trade updates from spot worker
	r.mu.Lock()
	defer r.mu.Unlock()

	if trade.Symbol != r.syncState.SpotWorker.Symbol() || trade.IsFutures {
		return
	}

	r.syncState.SpotWorker.AddTrade(trade)

	// try to transfer asset from spot to futures.
	// if transfer fails, retry in the next tick until it succeeds
	if err := r.futuresService.TransferFuturesAccountAsset(
		r.syncState.SpotWorker.ctx, r.syncState.Asset, trade.Quantity, types.TransferIn,
	); err != nil {
		r.logger.WithError(err).Errorf("failed to transfer %s %s from futures to spot",
			trade.Quantity, r.syncState.Asset)
		if _, found := r.syncState.RetryTransfers[trade.ID]; !found {
			bbgo.Notify(
				fmt.Errorf("transfer failed (%s %s), retrying: %w",
					trade.Quantity.String(),
					r.syncState.Asset,
					err,
				),
			)
		}
		r.syncState.RetryTransfers[trade.ID] = transferRetry{
			Trade:     trade,
			LastTried: currentTime,
		}
		return
	}
	// transfer succeeded, remove from retry list if exists
	delete(r.syncState.RetryTransfers, trade.ID)
	r.syncFuturesPosition(trade)
}

func (r *ArbitrageRound) HandleFuturesTrade(trade types.Trade, currentTime time.Time) {
	if trade.Symbol != r.syncState.FuturesWorker.Symbol() || !trade.IsFutures {
		return
	}
	r.logger.Infof("handling future trade: %s", trade)
	r.syncState.FuturesWorker.AddTrade(trade)
}

func (r *ArbitrageRound) SetLogger(logger logrus.FieldLogger) {
	r.logger = logger
}

func (r *ArbitrageRound) SpotSymbol() string {
	return r.syncState.SpotWorker.Symbol()
}

func (r *ArbitrageRound) FuturesSymbol() string {
	return r.syncState.FuturesWorker.Symbol()
}

func (r *ArbitrageRound) FuturesMarket() types.Market {
	return r.syncState.FuturesWorker.Market()
}

func (r *ArbitrageRound) State() RoundState {
	return r.syncState.State
}

func (r *ArbitrageRound) SetClosing(currentTime time.Time, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.syncState.SpotWorker.SetTargetPosition(fixedpoint.Zero)
	r.syncState.SpotWorker.ResetTime(currentTime, duration)
	r.syncState.FuturesWorker.SetTargetPosition(fixedpoint.Zero)
	r.syncState.FuturesWorker.ResetTime(currentTime, duration)

	r.syncState.State = RoundClosing
	r.syncState.ClosingTime = currentTime
	r.syncState.ClosingDuration = duration
}

func (r *ArbitrageRound) AnnualizedRate() fixedpoint.Value {
	return AnnualizedRate(r.syncState.TriggeredFundingRate, r.syncState.FundingIntervalHours)
}

func (r *ArbitrageRound) Tick(currentTime time.Time, spotOrderBook types.OrderBook, futuresOrderBook types.OrderBook) {
	r.mu.Lock()
	defer r.mu.Unlock()

	defer func() {
		// the state is PositionOpening
		// check if the spot and futures positions are fully filled -> PositionReady
		if r.syncState.State == RoundOpening {
			// get mid price
			spotBid, _ := spotOrderBook.BestBid()
			spotAsk, _ := spotOrderBook.BestAsk()
			futuresBid, _ := futuresOrderBook.BestBid()
			futuresAsk, _ := futuresOrderBook.BestAsk()
			spotMidPrice := spotBid.Price.Add(spotAsk.Price).Div(fixedpoint.Two)
			futuresMidPrice := futuresBid.Price.Add(futuresAsk.Price).Div(fixedpoint.Two)

			spotRemaining := r.syncState.SpotWorker.RemainingQuantity()
			futuresRemaining := r.syncState.FuturesWorker.RemainingQuantity()
			spotIsDust := r.syncState.SpotWorker.Market().IsDustQuantity(spotRemaining.Abs(), spotMidPrice)
			futuresIsDust := r.syncState.FuturesWorker.Market().IsDustQuantity(futuresRemaining.Abs(), futuresMidPrice)

			if spotIsDust && futuresIsDust {
				r.syncState.State = RoundReady
				return
			}
		}

		// the state is PositionClosing
		// check if the spot and futures positions are fully closed -> PositionClosed
		if r.syncState.State == RoundClosing {
			if r.syncState.SpotWorker.FilledPosition().IsZero() && r.syncState.FuturesWorker.FilledPosition().IsZero() {
				r.syncState.State = RoundClosed
				r.logger.Infof("positions closed, arbitrage round completed: %s", r.syncState.SpotWorker.Symbol())
			}
			return
		}
	}()

	if r.syncState.State == RoundPending {
		// not started yet, do nothing
		return
	}

	if r.logger == nil {
		r.logger = logrus.WithFields(logrus.Fields{
			"component": "ArbitrageRound",
			"symbol":    r.syncState.SpotWorker.Symbol(),
		})
	}

	if r.syncState.State == RoundClosed || r.syncState.State == RoundReady {
		return
	}

	r.retryTransferTickC <- currentTime

	// it's opening or closing, tick the workers
	r.syncState.SpotWorker.Tick(currentTime, spotOrderBook)
	r.syncState.FuturesWorker.Tick(currentTime, futuresOrderBook)
}

func (r *ArbitrageRound) syncFuturesPosition(trade types.Trade) {
	futureTargetPosition := r.syncState.FuturesWorker.TargetPosition()
	if r.syncState.SpotWorker.TargetPosition().Sign() > 0 {
		futureTargetPosition = futureTargetPosition.Sub(trade.Quantity)
	} else {
		futureTargetPosition = futureTargetPosition.Add(trade.Quantity)
	}
	r.logger.Infof("syncing futures position to %s", futureTargetPosition)
	r.syncState.FuturesWorker.SetTargetPosition(futureTargetPosition)
}
