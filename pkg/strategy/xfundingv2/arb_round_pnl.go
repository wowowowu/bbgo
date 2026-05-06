package xfundingv2

import (
	"context"
	"time"

	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

// TODO: add slack notification support for RoundPnL
type RoundPnL struct {
	FundingIncome      fixedpoint.Value
	SpotProfitStats    *types.ProfitStats
	FuturesProfitStats *types.ProfitStats
}

func (r *ArbitrageRound) PnL(ctx context.Context, currentTime time.Time) RoundPnL {
	r.mu.Lock()
	defer r.mu.Unlock()

	fundingIncome := r.collectedFunding(ctx, currentTime)

	spotMarket := r.spotWorker.Market()
	futuresMarket := r.futuresWorker.Market()

	spotPosition := types.NewPositionFromMarket(spotMarket)
	futuresPosition := types.NewPositionFromMarket(futuresMarket)

	spotProfitStats := types.NewProfitStats(spotMarket)
	futuresProfitStats := types.NewProfitStats(futuresMarket)

	spotTrades := r.spotWorker.Executor().AllTrades()
	for _, trade := range spotTrades {
		profit, netProfit, madeProfit := spotPosition.AddTrade(trade)
		if madeProfit {
			p := spotPosition.NewProfit(trade, profit, netProfit)
			spotProfitStats.AddProfit(p)
		}
	}

	futuresTrades := r.futuresWorker.Executor().AllTrades()
	for _, trade := range futuresTrades {
		profit, netProfit, madeProfit := futuresPosition.AddTrade(trade)
		if madeProfit {
			p := futuresPosition.NewProfit(trade, profit, netProfit)
			futuresProfitStats.AddProfit(p)
		}
	}

	return RoundPnL{
		FundingIncome:      fundingIncome,
		SpotProfitStats:    spotProfitStats,
		FuturesProfitStats: futuresProfitStats,
	}
}
