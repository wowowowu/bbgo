package xfundingv2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

func (s *Strategy) processPendingRounds(ctx context.Context, currentTime time.Time) {
	var pendingRounds []*ArbitrageRound
	for _, round := range s.pendingRounds {
		// moving round out from the pending list
		delete(s.pendingRounds, round.SpotSymbol())
		pendingRounds = append(pendingRounds, round)
	}
	var processedRounds []*ArbitrageRound
	for _, round := range pendingRounds {
		// calculate the fee asset required for the round
		if err := s.calculateRoundFeeAsset(round); err != nil {
			s.logger.WithError(err).Errorf(
				"failed to prepare fee asset for round: %s",
				round,
			)
			continue
		}
		processedRounds = append(processedRounds, round)
	}
	if err := s.acquireFeeAssetAndTransfer(ctx, processedRounds); err != nil {
		s.logger.WithError(err).Error("failed to acquire fee asset and transfer for pending rounds")
		return
	}

	for _, round := range processedRounds {
		if err := round.Start(ctx, currentTime); err != nil {
			s.logger.WithError(err).Errorf(
				"failed to start round after fee asset preparation: %s",
				round,
			)
			continue
		}
		s.activeRounds[round.SpotSymbol()] = round
		s.logger.Infof("round started: %s", round)
	}
}

func (s *Strategy) acquireFeeAssetAndTransfer(ctx context.Context, rounds []*ArbitrageRound) error {
	var requiredSpotFeeAmount, requiredFuturesFeeAmount fixedpoint.Value
	for _, round := range rounds {
		requiredSpotFeeAmount = requiredSpotFeeAmount.Add(round.SpotFeeAssetAmount())
		requiredFuturesFeeAmount = requiredFuturesFeeAmount.Add(round.FuturesFeeAssetAmount())
	}
	spotAccount := s.spotSession.GetAccount()
	futuresAccount := s.futuresSession.GetAccount()
	market, _ := s.spotSession.Market(s.FeeSymbol)
	spotFeeBalance, _ := spotAccount.Balance(market.BaseCurrency)
	futuresFeeBalance, _ := futuresAccount.Balance(market.BaseCurrency)
	spotDeficit := requiredSpotFeeAmount.Sub(spotFeeBalance.Available)
	futuresDeficit := requiredFuturesFeeAmount.Sub(futuresFeeBalance.Available)
	var buyQuantity, transferAmount fixedpoint.Value
	var transferDirection types.TransferDirection
	if spotDeficit.Add(futuresDeficit).Sign() >= 0 {
		// this case means the total fee asset balance is not sufficient
		// need to buy and transfer in the fee asset to cover the deficit
		buyQuantity = spotDeficit.Add(futuresDeficit) // must be positive or zero
		transferAmount = fixedpoint.Max(fixedpoint.Zero, futuresDeficit)
		transferDirection = types.TransferIn
	} else {
		// this case means the total fee asset balance is sufficient
		if futuresDeficit.Sign() > 0 {
			transferAmount = futuresDeficit
			transferDirection = types.TransferIn
		}
		if spotDeficit.Sign() > 0 {
			transferAmount = spotDeficit
			transferDirection = types.TransferOut
		}
	}
	if !buyQuantity.IsZero() {
		orderExecutor, found := s.spotGeneralOrderExecutors[s.FeeSymbol]
		if !found {
			return fmt.Errorf("no order executor found for fee symbol %s", s.FeeSymbol)
		}
		if _, err := orderExecutor.SubmitOrders(ctx, types.SubmitOrder{
			Symbol:   s.FeeSymbol,
			Side:     types.SideTypeBuy,
			Type:     types.OrderTypeMarket,
			Quantity: buyQuantity,
		}); err != nil {
			return fmt.Errorf("failed to buy fee asset for pending rounds: %w", err)
		}
	}
	if !transferAmount.IsZero() {
		if err := s.futuresService.TransferFuturesAccountAsset(ctx, market.BaseCurrency, transferAmount, transferDirection); err != nil {
			return fmt.Errorf("failed to transfer %s %s for pending rounds fee preparation: %w", transferAmount, market.BaseCurrency, err)
		}
	}
	return nil
}

func (s *Strategy) calculateRoundFeeAsset(round *ArbitrageRound) error {
	feeOrderBook, ok := s.spotOrderBooks[s.FeeSymbol]
	if !ok {
		return nil
	}

	spotFeeRate := s.spotSession.TakerFeeRate
	futuresFeeRate := s.futuresSession.TakerFeeRate

	// calculate the spot/futures leg fee assets
	var spotFeeAmount, futuresFeeAmount fixedpoint.Value
	targetPosition := round.TargetPosition()
	positionSize := targetPosition.Abs()

	// get fee asset price (we need to buy it, so use the sell/ask side)
	feeSellBook := feeOrderBook.SideBook(types.SideTypeSell)
	var spotBasePrice, futuresBasePrice fixedpoint.Value
	if targetPosition.Sign() > 0 {
		// long spot (buy at ask side), short futures (sell at bid side)
		spotSellBook := s.spotOrderBooks[round.SpotSymbol()].SideBook(types.SideTypeSell)
		futuresBuyBook := s.futuresOrderBooks[round.FuturesSymbol()].SideBook(types.SideTypeBuy)

		spotBasePrice = spotSellBook.AverageDepthPrice(positionSize)
		futuresBasePrice = futuresBuyBook.AverageDepthPrice(positionSize)
		if spotBasePrice.IsZero() || futuresBasePrice.IsZero() {
			return errors.New("order book data is not ready yet")
		}
	} else {
		// short spot (sell at bid side), long futures (buy at ask side)
		spotBuyBook := s.spotOrderBooks[round.SpotSymbol()].SideBook(types.SideTypeBuy)
		futuresSellBook := s.futuresOrderBooks[round.FuturesSymbol()].SideBook(types.SideTypeSell)

		spotBasePrice = spotBuyBook.AverageDepthPrice(positionSize)
		futuresBasePrice = futuresSellBook.AverageDepthPrice(positionSize)
		if spotBasePrice.IsZero() || futuresBasePrice.IsZero() {
			return errors.New("order book data is not ready yet")
		}
	}
	spotFeeInQuote := spotBasePrice.Mul(positionSize).Mul(spotFeeRate)
	futuresFeeInQuote := futuresBasePrice.Mul(positionSize).Mul(futuresFeeRate)
	totalFeeInQuote := spotFeeInQuote.Add(futuresFeeInQuote)
	feeAssetPrice := feeSellBook.AverageDepthPriceByQuote(totalFeeInQuote, 0)
	spotFeeAmount = spotFeeInQuote.Div(feeAssetPrice)
	futuresFeeAmount = futuresFeeInQuote.Div(feeAssetPrice)

	round.SetSpotFeeAssetAmount(spotFeeAmount)
	round.SetFuturesFeeAssetAmount(futuresFeeAmount)

	return nil
}
