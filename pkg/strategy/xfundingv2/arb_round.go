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
	Trade     types.Trade
	LastTried time.Time
}

type ArbitrageRound struct {
	mu sync.Mutex

	triggeredFundingRate fixedpoint.Value
	minHoldingIntervals  int
	fundingIntervalHours int

	// TWAP workers
	spotWorker     *TWAPWorker
	futuresWorker  *TWAPWorker
	futuresService FuturesService
	asset          string // base asset, e.g. "BTC"

	retryDuration      time.Duration
	retryTransferTickC chan time.Time
	retryTransfers     map[uint64]transferRetry

	fundingFeeRecords map[int64]FundingFee

	state RoundState

	logger logrus.FieldLogger

	// startTime is the time when the round is started
	startTime time.Time
	// closingTime is the time when the round is entered closing state
	closingTime     time.Time
	closingDuration time.Duration
}

func NewArbitrageRound(
	fundingRate fixedpoint.Value, minHoldingIntervals, fundingIntervalHours int,
	spotTwap, futuresTwap *TWAPWorker, futuresService FuturesService) *ArbitrageRound {
	return &ArbitrageRound{
		triggeredFundingRate: fundingRate,
		minHoldingIntervals:  minHoldingIntervals,
		fundingIntervalHours: fundingIntervalHours,
		spotWorker:           spotTwap,
		futuresWorker:        futuresTwap,
		futuresService:       futuresService,
		asset:                spotTwap.Market().BaseCurrency,
		state:                RoundPending,
		retryTransfers:       make(map[uint64]transferRetry),
		retryTransferTickC:   make(chan time.Time, 1),
		fundingFeeRecords:    make(map[int64]FundingFee),
	}
}

func (r *ArbitrageRound) StartTime() time.Time {
	return r.startTime
}

func (r *ArbitrageRound) IsClosable(currentTime time.Time) bool {
	if r.startTime.IsZero() {
		return false
	}
	intervalDuration := time.Duration(r.fundingIntervalHours) * time.Hour
	intervalStart := r.startTime.Truncate(intervalDuration)
	lastIntervalEnd := currentTime.Truncate(intervalDuration)
	numIntervalPassed := int(lastIntervalEnd.Sub(intervalStart) / intervalDuration)
	return numIntervalPassed > r.minHoldingIntervals
}

func (r *ArbitrageRound) String() string {
	if r.state != RoundClosing {
		return fmt.Sprintf(
			"ArbitrageRound(symbol=%s, state=%s, spot=%s, futures=%s, startTime=%s)",
			r.spotWorker.Symbol(),
			r.state,
			r.spotWorker.FilledPosition(),
			r.futuresWorker.FilledPosition(),
			r.startTime.Format(time.RFC3339),
		)
	}
	return fmt.Sprintf(
		"ArbitrageRound(symbol=%s, state=%s, spot=%s, futures=%s, closingTime=%s, expectedCloseTime=%s)",
		r.spotWorker.Symbol(),
		r.state,
		r.spotWorker.FilledPosition(),
		r.futuresWorker.FilledPosition(),
		r.closingTime.Format(time.RFC3339),
		r.closingTime.Add(r.closingDuration).Format(time.RFC3339),
	)
}

func (r *ArbitrageRound) CollectedFunding(ctx context.Context, currentTime time.Time) fixedpoint.Value {
	if r.startTime.IsZero() {
		return fixedpoint.Zero
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.syncFundingFeeRecords(ctx, currentTime)

	var totalFunding fixedpoint.Value
	for _, fee := range r.fundingFeeRecords {
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
		"spot":    r.spotWorker.Executor().AllOrders(),
		"futures": r.futuresWorker.Executor().AllOrders(),
	}

	return orders
}

func (r *ArbitrageRound) Trades() map[string][]types.Trade {
	r.mu.Lock()
	defer r.mu.Unlock()

	trades := map[string][]types.Trade{
		"spot":    r.spotWorker.Executor().AllTrades(),
		"futures": r.futuresWorker.Executor().AllTrades(),
	}

	return trades
}

func (r *ArbitrageRound) HasOrder(orderID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, spotExists := r.spotWorker.Executor().GetOrder(orderID)
	_, futuresExists := r.futuresWorker.Executor().GetOrder(orderID)

	return spotExists || futuresExists
}

func (r *ArbitrageRound) syncFundingFeeRecords(ctx context.Context, currentTime time.Time) {
	if r.startTime.IsZero() || r.startTime.After(currentTime) {
		return
	}

	q := batch.BinanceFuturesIncomeBatchQuery{
		BinanceFuturesIncomeHistoryService: r.futuresService,
	}
	symbol := r.futuresWorker.Symbol()
	dataC, errC := q.Query(ctx, symbol, binanceapi.FuturesIncomeFundingFee, r.startTime, currentTime)
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
				r.fundingFeeRecords[income.TranId] = record
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
	if r.startTime.IsZero() {
		if err := r.spotWorker.Start(ctx, currentTime); err != nil {
			return fmt.Errorf("failed to start spot worker: %w", err)
		}
		if err := r.futuresWorker.Start(ctx, currentTime); err != nil {
			return fmt.Errorf("failed to start futures worker: %w", err)
		}

		go r.retryTransferWorker(ctx)

		r.startTime = currentTime
		r.state = RoundOpening
	}
	return nil
}

func (r *ArbitrageRound) Stop() {
	r.spotWorker.Stop()
	r.futuresWorker.Stop()
	close(r.retryTransferTickC)
}

func (r *ArbitrageRound) retryTransferWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case currentTime, ok := <-r.retryTransferTickC:
			if !ok {
				return
			}
			// retry failed transfers if any
			r.mu.Lock()
			for tradeID, transfer := range r.retryTransfers {
				retryDuration := r.retryDuration
				if retryDuration == 0 {
					// default retry duration is 10 minutes
					retryDuration = 10 * time.Minute
				}
				if currentTime.Sub(transfer.LastTried) < retryDuration {
					continue
				}
				r.HandleSpotTrade(transfer.Trade, currentTime)
				r.logger.Infof("retry transfer succeeded (trade: %d): %s %s", tradeID, transfer.Trade.Quantity.String(), r.asset)
			}
			r.mu.Unlock()
		}
	}
}

func (r *ArbitrageRound) SetRetryDuration(d time.Duration) {
	r.retryDuration = d
}

func (r *ArbitrageRound) HandleSpotTrade(trade types.Trade, currentTime time.Time) {
	// lock the round to ensure the state is updated correctly when receiving trade updates from spot worker
	r.mu.Lock()
	defer r.mu.Unlock()

	if trade.Symbol != r.spotWorker.Symbol() || trade.IsFutures {
		return
	}

	// try to transfer asset from futures to spot.
	// if transfer fails, retry in the next tick until it succeeds
	if err := r.futuresService.TransferFuturesAccountAsset(
		r.spotWorker.ctx, r.asset, trade.Quantity, types.TransferOut,
	); err != nil {
		r.logger.WithError(err).Errorf("failed to transfer %s %s from futures to spot",
			trade.Quantity.String(), r.asset)
		if _, found := r.retryTransfers[trade.ID]; !found {
			bbgo.Notify(
				fmt.Errorf("transfer failed (%s %s), retrying: %w",
					trade.Quantity.String(),
					r.asset,
					err,
				),
			)
		}
		r.retryTransfers[trade.ID] = transferRetry{
			Trade:     trade,
			LastTried: currentTime,
		}
		return
	}
	// transfer succeeded, remove from retry list if exists
	delete(r.retryTransfers, trade.ID)
	r.syncFuturesPosition(trade)
}

func (r *ArbitrageRound) SetLogger(logger logrus.FieldLogger) {
	r.logger = logger
}

func (r *ArbitrageRound) SpotSymbol() string {
	return r.spotWorker.Symbol()
}

func (r *ArbitrageRound) FuturesSymbol() string {
	return r.futuresWorker.Symbol()
}

func (r *ArbitrageRound) State() RoundState {
	return r.state
}

func (r *ArbitrageRound) SetClosing(currentTime time.Time, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.spotWorker.SetTargetPosition(fixedpoint.Zero)
	r.spotWorker.ResetTime(currentTime, duration)
	r.futuresWorker.SetTargetPosition(fixedpoint.Zero)
	r.futuresWorker.ResetTime(currentTime, duration)

	r.state = RoundClosing
	r.closingTime = currentTime
	r.closingDuration = duration
}

func (r *ArbitrageRound) AnnualizedRate() fixedpoint.Value {
	return AnnualizedRate(r.triggeredFundingRate, r.fundingIntervalHours)
}

func (r *ArbitrageRound) Tick(currentTime time.Time, spotOrderBook types.OrderBook, futuresOrderBook types.OrderBook) {
	r.mu.Lock()
	defer r.mu.Unlock()

	defer func() {
		// the state is PositionOpening
		// check if the spot and futures positions are fully filled -> PositionReady
		if r.state == RoundOpening {
			// get mid price
			spotBid, _ := spotOrderBook.BestBid()
			spotAsk, _ := spotOrderBook.BestAsk()
			futuresBid, _ := futuresOrderBook.BestBid()
			futuresAsk, _ := futuresOrderBook.BestAsk()
			spotMidPrice := spotBid.Price.Add(spotAsk.Price).Div(fixedpoint.Two)
			futuresMidPrice := futuresBid.Price.Add(futuresAsk.Price).Div(fixedpoint.Two)

			spotRemaining := r.spotWorker.RemainingQuantity()
			futuresRemaining := r.futuresWorker.RemainingQuantity()
			spotIsDust := r.spotWorker.Market().IsDustQuantity(spotRemaining.Abs(), spotMidPrice)
			futuresIsDust := r.futuresWorker.Market().IsDustQuantity(futuresRemaining.Abs(), futuresMidPrice)

			if spotIsDust && futuresIsDust {
				r.state = RoundReady
				return
			}
		}

		// the state is PositionClosing
		// check if the spot and futures positions are fully closed -> PositionClosed
		if r.state == RoundClosing {
			if r.spotWorker.FilledPosition().IsZero() && r.futuresWorker.FilledPosition().IsZero() {
				r.state = RoundClosed
				r.logger.Infof("positions closed, arbitrage round completed: %s", r.spotWorker.Symbol())
			}
			return
		}
	}()

	if r.state == RoundPending {
		// not started yet, do nothing
		return
	}

	if r.logger == nil {
		r.logger = logrus.WithFields(logrus.Fields{
			"component": "ArbitrageRound",
			"symbol":    r.spotWorker.Symbol(),
		})
	}

	if r.state == RoundClosed || r.state == RoundReady {
		return
	}

	r.retryTransferTickC <- currentTime

	// it's opening or closing, tick the workers
	r.spotWorker.Tick(currentTime, spotOrderBook)
	r.futuresWorker.Tick(currentTime, futuresOrderBook)
}

func (r *ArbitrageRound) syncFuturesPosition(trade types.Trade) {
	futureTargetPosition := r.futuresWorker.TargetPosition()
	if r.spotWorker.TargetPosition().Sign() > 0 {
		futureTargetPosition = futureTargetPosition.Sub(trade.Quantity)
	} else {
		futureTargetPosition = futureTargetPosition.Add(trade.Quantity)
	}
	r.logger.Infof("syncing futures position to %s", futureTargetPosition)
	r.futuresWorker.SetTargetPosition(futureTargetPosition)
}
