package xfundingv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

func (r *ArbitrageRound) LoadStrategy(ctx context.Context, s *Strategy) error {
	r.SetLogger(s.logger)
	r.SetFuturesExchangeFeeRates(
		map[types.ExchangeName]types.ExchangeFee{
			s.futuresSession.Exchange.Name(): {
				MakerFeeRate: s.futuresSession.MakerFeeRate,
				TakerFeeRate: s.futuresSession.TakerFeeRate,
			},
		},
	)
	r.SetSpotExchangeFeeRates(
		map[types.ExchangeName]types.ExchangeFee{
			s.spotSession.Exchange.Name(): {
				MakerFeeRate: s.spotSession.MakerFeeRate,
				TakerFeeRate: s.spotSession.TakerFeeRate,
			},
		},
	)
	r.retryTransferTickC = make(chan time.Time)
	if !r.HasStarted() {
		// the round has been started before, we need to start the retry worker
		go r.retryTransferWorker(ctx, r.retryTransferTickC)
	}
	if service, ok := s.futuresSession.Exchange.(FuturesService); ok {
		r.futuresService = service
	} else {
		return errors.New("[ArbitrageRound] futures exchange does not implement FuturesService")
	}
	if r.syncState.SpotWorker != nil {
		if err := r.syncState.SpotWorker.LoadStrategy(ctx, s); err != nil {
			return fmt.Errorf("[ArbitrageRound] spot load strategy error: %w", err)
		}
	} else {
		// should not happend
		// by the time we create the round, the spot worker is never nil
		// the restored round should always have the spot worker restored as well.
		return errors.New("[ArbitrageRound] spot worker is nil")
	}
	if r.syncState.FuturesWorker != nil {
		if err := r.syncState.FuturesWorker.LoadStrategy(ctx, s); err != nil {
			return fmt.Errorf("[ArbitrageRound] futures load strategy error: %w", err)
		}
	} else {
		// should not happend
		// by the time we create the round, the futures worker is never nil
		// the restored round should always have the futures worker restored as well.
		return errors.New("[ArbitrageRound] futures worker is nil")
	}

	return nil
}

type ArbitrageRoundSyncState struct {
	TriggeredFundingRate        fixedpoint.Value     `json:"triggeredFundingRate"`
	TriggeredSpotTargetPosition fixedpoint.Value     `json:"triggeredSpotTargetPosition"`
	MinHoldingIntervals         int                  `json:"minHoldingIntervals"`
	FundingIntervalHours        int                  `json:"fundingIntervalHours"`
	FundingIntervalStart        time.Time            `json:"fundingIntervalStart"`
	FundingIntervalEnd          time.Time            `json:"fundingIntervalEnd"`
	FundingFeeRecords           map[int64]FundingFee `json:"fundingFeeRecords"`

	Symbol        string      `json:"symbol"`
	SpotWorker    *TWAPWorker `json:"spotWorker,omitempty"`
	FuturesWorker *TWAPWorker `json:"futuresWorker,omitempty"`
	Asset         string      `json:"asset"` // base asset, e.g. "BTC"

	SpotFeeAssetAmount    fixedpoint.Value `json:"spotFeeAssetAmount"`
	FuturesFeeAssetAmount fixedpoint.Value `json:"futuresFeeAssetAmount"`
	FeeSymbol             string           `json:"feeSymbol"`
	AvgFeeCost            fixedpoint.Value `json:"avgFeeCost"`

	RetryDuration  time.Duration            `json:"retryDuration"`
	RetryTransfers map[uint64]transferRetry `json:"retryTransfers"`

	State RoundState `json:"state"`

	// StartTime is the time when the round is started
	StartTime time.Time `json:"startTime"`
	// ClosingTime is the time when the round is entered closing state
	ClosingTime     time.Time     `json:"closingTime"`
	ClosingDuration time.Duration `json:"closingDuration"`
	// LastUpdateTime is the last time when the round is updated
	LastUpdateTime time.Time `json:"lastUpdateTime"`
}

func (r *ArbitrageRound) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.syncState)
}

func (r *ArbitrageRound) UnmarshalJSON(b []byte) error {
	syncState := ArbitrageRoundSyncState{}
	if err := json.Unmarshal(b, &syncState); err != nil {
		return err
	}

	r.syncState = syncState
	return nil
}
