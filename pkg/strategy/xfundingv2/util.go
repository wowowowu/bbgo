package xfundingv2

import (
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

func getMidPrice(orderBook types.OrderBook) fixedpoint.Value {
	bestBid, _ := orderBook.BestBid()
	bestAsk, _ := orderBook.BestAsk()
	return bestBid.Price.Add(bestAsk.Price).Div(fixedpoint.Two)
}
