package xfundingv2

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

//go:generate stringer -type=RoundState

type RoundState int

const (
	PositionOpening RoundState = iota
	PositionReady
	PositionClosing
	PositionClosed
)

type FuturesTransfer interface {
	TransferFuturesAccountAsset(ctx context.Context, asset string, amount fixedpoint.Value, io types.TransferDirection) error
	QueryAccountBalances(ctx context.Context) (types.BalanceMap, error)
}

type transferRetry struct {
	Trade     types.Trade
	LastTried time.Time
}

type ArbitrageRound struct {
	mu sync.Mutex

	CollectedFunding     fixedpoint.Value
	FundingRate          fixedpoint.Value
	FundingIntervalHours int

	// TWAP workers
	spotWorker      *TWAPWorker
	futuresWorker   *TWAPWorker
	futuresTransfer FuturesTransfer
	transferAsset   string // base asset, e.g. "BTC"

	retryDuration      time.Duration
	retryTransferTickC chan time.Time
	retryTransfers     map[uint64]transferRetry

	state RoundState

	logger logrus.FieldLogger

	startTime time.Time
}

func NewArbitrageRound(spotTwap, futuresTwap *TWAPWorker, futuresTransfer FuturesTransfer) *ArbitrageRound {
	return &ArbitrageRound{
		spotWorker:         spotTwap,
		futuresWorker:      futuresTwap,
		futuresTransfer:    futuresTransfer,
		transferAsset:      spotTwap.Market().BaseCurrency,
		state:              PositionOpening,
		retryTransfers:     make(map[uint64]transferRetry),
		retryTransferTickC: make(chan time.Time, 1),
	}
}

func (p *ArbitrageRound) Start(ctx context.Context, currentTime time.Time) {
	p.spotWorker.Start(ctx, currentTime)
	p.futuresWorker.Start(ctx, currentTime)
	go p.retryTransferWorker(ctx)
}

func (p *ArbitrageRound) Stop() {
	p.spotWorker.Stop()
	p.futuresWorker.Stop()
	close(p.retryTransferTickC)
}

func (p *ArbitrageRound) retryTransferWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case currentTime, ok := <-p.retryTransferTickC:
			if !ok {
				return
			}
			// retry failed transfers if any
			p.mu.Lock()
			for tradeID, transfer := range p.retryTransfers {
				retryDuration := p.retryDuration
				if retryDuration == 0 {
					// default retry duration is 10 minutes
					retryDuration = 10 * time.Minute
				}
				if currentTime.Sub(transfer.LastTried) < retryDuration {
					continue
				}
				p.HandleSpotTrade(transfer.Trade, currentTime)
				p.logger.Infof("retry transfer succeeded (trade: %d): %s %s", tradeID, transfer.Amount.String(), p.transferAsset)
			}
			p.mu.Unlock()
		}
	}
}

func (p *ArbitrageRound) SetRetryDuration(d time.Duration) {
	p.retryDuration = d
}

func (p *ArbitrageRound) HandleSpotTrade(trade types.Trade, currentTime time.Time) {
	// lock the round to ensure the state is updated correctly when receiving trade updates from spot worker
	p.mu.Lock()
	defer p.mu.Unlock()

	if trade.Symbol != p.spotWorker.Symbol() || trade.IsFutures {
		return
	}

	// try to transfer asset from futures to spot.
	// if transfer fails, retry in the next tick until it succeeds
	if err := p.futuresTransfer.TransferFuturesAccountAsset(
		p.spotWorker.ctx, p.transferAsset, trade.Quantity, types.TransferOut,
	); err != nil {
		p.logger.WithError(err).Errorf("failed to transfer %s %s from futures to spot",
			trade.Quantity.String(), p.transferAsset)
		if _, found := p.retryTransfers[trade.ID]; !found {
			bbgo.Notify(
				fmt.Errorf("transfer failed (%s %s), retrying: %w",
					trade.Quantity.String(),
					p.transferAsset,
					err,
				),
			)
		}
		p.retryTransfers[trade.ID] = transferRetry{
			Trade:     trade,
			LastTried: currentTime,
		}
		return
	}
	// transfer succeeded, remove from retry list if exists
	delete(p.retryTransfers, trade.ID)
	p.syncFuturesPosition(trade)
}

func (p *ArbitrageRound) SetLogger(logger logrus.FieldLogger) {
	p.logger = logger
}

func (p *ArbitrageRound) SpotSymbol() string {
	return p.spotWorker.Symbol()
}

func (p *ArbitrageRound) FuturesSymbol() string {
	return p.futuresWorker.Symbol()
}

func (p *ArbitrageRound) FuturesTargetPosition() fixedpoint.Value {
	return p.spotWorker.FilledQuantity().Neg()
}

func (p *ArbitrageRound) GetState() RoundState {
	return p.state
}

func (p *ArbitrageRound) AnnualizedRate() fixedpoint.Value {
	return AnnualizedRate(p.FundingRate, p.FundingIntervalHours)
}

func (p *ArbitrageRound) Tick(currentTime time.Time, spotOrderBook types.OrderBook, futuresOrderBook types.OrderBook) {
	p.mu.Lock()
	defer p.mu.Unlock()

	defer func() {
		// the state is PositionOpening
		// check if the spot and futures positions are fully filled -> PositionReady
		if p.state == PositionOpening {
			targetPosition := p.spotWorker.TargetPosition()
			spotDiff := targetPosition.Sub(p.spotWorker.FilledQuantity())
			futuresDiff := targetPosition.Neg().Sub(p.futuresWorker.FilledQuantity())
			if spotDiff.IsZero() && futuresDiff.IsZero() {
				p.state = PositionReady
				return
			}
		}

		// the state is PositionClosing
		// check if the spot and futures positions are fully closed -> PositionClosed
		if p.state == PositionClosing {
			if p.spotWorker.FilledQuantity().IsZero() && p.futuresWorker.FilledQuantity().IsZero() {
				p.state = PositionClosed
				p.logger.Infof("positions closed, arbitrage round completed: %s", p.spotWorker.Symbol())
			}
			return
		}
	}()

	if p.startTime.IsZero() {
		p.startTime = currentTime
	}

	if p.logger == nil {
		p.logger = logrus.WithFields(logrus.Fields{
			"component": "ArbitrageRound",
			"symbol":    p.spotWorker.Symbol(),
		})
	}

	if p.state == PositionClosed || p.state == PositionReady {
		return
	}

	p.retryTransferTickC <- currentTime

	// it's opening or closing, tick the workers
	p.spotWorker.Tick(currentTime, spotOrderBook)
	p.futuresWorker.Tick(currentTime, futuresOrderBook)
}

func (p *ArbitrageRound) syncFuturesPosition(trade types.Trade) {
	futureTargetPosition := p.futuresWorker.TargetPosition()
	if p.spotWorker.TargetPosition().Sign() > 0 {
		futureTargetPosition = futureTargetPosition.Sub(trade.Quantity)
	} else {
		futureTargetPosition = futureTargetPosition.Add(trade.Quantity)
	}
	p.logger.Infof("syncing futures position to %s", futureTargetPosition)
	p.futuresWorker.SetTargetPosition(futureTargetPosition)
}
