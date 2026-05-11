package xfundingv2

import (
	"context"
	"fmt"
	"time"

	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
	"github.com/sirupsen/logrus"
)

type TWAPExecutor struct {
	ctx      context.Context
	exchange types.ExchangeOrderQueryService
	executor *bbgo.GeneralOrderExecutor

	syncState TWAPExecutorSyncState

	logger logrus.FieldLogger
}

func NewTWAPExecutor(
	ctx context.Context,
	exchange types.ExchangeOrderQueryService,
	isFutures bool,
	market types.Market,
	executor *bbgo.GeneralOrderExecutor,
	config TWAPWorkerConfig,
) *TWAPExecutor {
	return &TWAPExecutor{
		ctx:      ctx,
		exchange: exchange,
		executor: executor,

		syncState: TWAPExecutorSyncState{
			Config:    config,
			Market:    market,
			IsFutures: isFutures,
			Orders:    make(map[uint64]types.OrderQuery),
			Trades:    make(map[uint64]types.Trade),
		},
	}
}

func (o *TWAPExecutor) SetLogger(logger logrus.FieldLogger) {
	o.logger = logger
}

func (o *TWAPExecutor) Market() types.Market {
	return o.syncState.Market
}

func (o *TWAPExecutor) Start() {
	if o.logger == nil {
		o.logger = logrus.WithFields(
			logrus.Fields{
				"component": "TWAPOrderExecutor",
				"symbol":    o.syncState.Market.Symbol,
			},
		)
	}
}

// AddTrade adds a trade to the executor's internal trade list if it belongs to
// an order managed by this executor.
func (o *TWAPExecutor) AddTrade(trade types.Trade) {
	if _, exists := o.syncState.Orders[trade.OrderID]; exists {
		o.syncState.Trades[trade.ID] = trade
	}
}

func (o *TWAPExecutor) Stop() error {
	// adding Stop method for future use
	return nil
}

// SyncOrder queries the latest order status and updates the internal store and trades.
// it's designed to be called to restore the state of the executor after a restart or to sync a existing order.
// it's not thread-safe
func (o *TWAPExecutor) SyncOrder(order types.Order) error {
	storeOrder, exists := o.executor.OrderStore().Get(order.OrderID)
	if exists && storeOrder.GetRemainingQuantity().IsZero() {
		return nil
	}

	timedCtx, cancel := context.WithTimeout(o.ctx, 500*time.Millisecond)
	defer cancel()

	query := order.AsQuery()
	updatedOrder, err := o.exchange.QueryOrder(
		timedCtx,
		query,
	)
	if err != nil {
		return fmt.Errorf("failed to query order: %v", query)
	}

	trades, err := o.exchange.QueryOrderTrades(timedCtx, query)
	if err != nil {
		return fmt.Errorf("failed to query order trades: %v, %v", query, err)
	}

	orderStore := o.executor.OrderStore()
	if !exists {
		orderStore.AddOrderUpdate = true
	}
	orderStore.HandleOrderUpdate(*updatedOrder)
	orderStore.AddOrderUpdate = false
	tradeCollector := o.executor.TradeCollector()
	for _, trade := range trades {
		tradeCollector.ProcessTrade(trade)
	}
	tradeCollector.Process()

	return nil
}

func (o *TWAPExecutor) GetOrder(orderID uint64) (types.Order, bool) {
	if _, exists := o.syncState.Orders[orderID]; !exists {
		return types.Order{}, false
	}
	return o.executor.OrderStore().Get(orderID)
}

func (o *TWAPExecutor) AllOrders() []types.Order {
	var orders []types.Order

	for orderID := range o.syncState.Orders {
		if order, exists := o.executor.OrderStore().Get(orderID); exists {
			orders = append(orders, order)
		}
	}
	return orders
}

func (o *TWAPExecutor) AllTrades() []types.Trade {
	var trades []types.Trade
	for _, trade := range o.syncState.Trades {
		trades = append(trades, trade)
	}
	return trades
}

// place order
func (o *TWAPExecutor) PlaceOrder(quantity fixedpoint.Value, side types.SideType, orderBook types.OrderBook, deadlineExceeded bool) (*types.Order, error) {
	// find the better price and submit new order
	quantity = o.syncState.Market.TruncateQuantity(quantity)
	price, err := o.GetPrice(side, orderBook)
	if err != nil {
		o.logger.WithError(err).Warn("[TWAP tick] failed to get price for active order update")
		return nil, err
	}
	price = o.syncState.Market.TruncatePrice(price)
	order := o.buildSubmitOrder(quantity, price, side, deadlineExceeded)
	if o.syncState.Market.IsDustQuantity(order.Quantity, order.Price) {
		return nil, fmt.Errorf("order is of dust quantity: %s", quantity)
	}

	timedCtx, cancel := context.WithTimeout(o.ctx, 500*time.Millisecond)
	defer cancel()

	createdOrders, err := o.executor.SubmitOrders(timedCtx, order)
	if err != nil || len(createdOrders) == 0 {
		return nil, fmt.Errorf("failed to submit order: %+v, %v", order, err)
	}
	o.syncState.Orders[createdOrders[0].OrderID] = createdOrders[0].AsQuery()
	return &createdOrders[0], nil
}

func (o *TWAPExecutor) buildSubmitOrder(quantity, price fixedpoint.Value, side types.SideType, deadlineExceeded bool) types.SubmitOrder {
	if deadlineExceeded {
		return types.SubmitOrder{
			Symbol:   o.syncState.Market.Symbol,
			Market:   o.syncState.Market,
			Side:     side,
			Type:     types.OrderTypeMarket,
			Quantity: quantity,
		}
	}
	orderType := types.OrderTypeLimitMaker
	timeInForce := types.TimeInForceGTC

	if o.syncState.Config.OrderType == TWAPOrderTypeTaker {
		orderType = types.OrderTypeLimit
		timeInForce = types.TimeInForceIOC
	}

	return types.SubmitOrder{
		Symbol:      o.syncState.Market.Symbol,
		Market:      o.syncState.Market,
		Side:        side,
		Type:        orderType,
		Quantity:    quantity,
		Price:       price,
		TimeInForce: timeInForce,
	}
}

func (o *TWAPExecutor) GetPrice(side types.SideType, orderBook types.OrderBook) (price fixedpoint.Value, err error) {
	defer func() {
		if err == nil {
			price = o.syncState.Market.TruncatePrice(price)
		}
	}()

	switch o.syncState.Config.OrderType {
	case TWAPOrderTypeTaker:
		return o.getTakerPrice(side, orderBook)
	case TWAPOrderTypeMaker:
		return o.getMakerPrice(side, orderBook)
	default:
		return o.getTakerPrice(side, orderBook)
	}
}

func (o *TWAPExecutor) getTakerPrice(side types.SideType, orderBook types.OrderBook) (fixedpoint.Value, error) {
	switch side {
	case types.SideTypeBuy:
		ask, ok := orderBook.BestAsk()
		if !ok {
			return fixedpoint.Zero, fmt.Errorf("no ask price available for %s", o.syncState.Market.Symbol)
		}
		price := ask.Price
		if o.syncState.Config.MaxSlippage.Sign() > 0 {
			maxPrice := price.Mul(fixedpoint.One.Add(o.syncState.Config.MaxSlippage))
			price = fixedpoint.Min(price, maxPrice)
		}
		return price, nil

	case types.SideTypeSell:
		bid, ok := orderBook.BestBid()
		if !ok {
			return fixedpoint.Zero, fmt.Errorf("no bid price available for %s", o.syncState.Market.Symbol)
		}
		price := bid.Price
		if o.syncState.Config.MaxSlippage.Sign() > 0 {
			minPrice := price.Mul(fixedpoint.One.Sub(o.syncState.Config.MaxSlippage))
			price = fixedpoint.Max(price, minPrice)
		}
		return price, nil

	default:
		return fixedpoint.Zero, fmt.Errorf("unknown side: %s", side)
	}
}

func (o *TWAPExecutor) getMakerPrice(side types.SideType, orderBook types.OrderBook) (fixedpoint.Value, error) {
	tickSize := o.syncState.Market.TickSize
	numOfTicks := fixedpoint.NewFromInt(int64(o.syncState.Config.NumOfTicks))
	tickImprovement := tickSize.Mul(numOfTicks)

	switch side {
	case types.SideTypeBuy:
		bid, ok := orderBook.BestBid()
		if !ok {
			return fixedpoint.Zero, fmt.Errorf("no bid price available for %s", o.syncState.Market.Symbol)
		}
		// improve price by moving closer to spread
		price := bid.Price.Add(tickImprovement)
		// but don't cross the spread
		ask, hasAsk := orderBook.BestAsk()
		if hasAsk && price.Compare(ask.Price) >= 0 {
			price = ask.Price.Sub(tickSize)
		}
		return price, nil

	case types.SideTypeSell:
		ask, ok := orderBook.BestAsk()
		if !ok {
			return fixedpoint.Zero, fmt.Errorf("no ask price available for %s", o.syncState.Market.Symbol)
		}
		price := ask.Price.Sub(tickImprovement)
		bid, hasBid := orderBook.BestBid()
		if hasBid && price.Compare(bid.Price) <= 0 {
			price = bid.Price.Add(tickSize)
		}
		return price, nil

	default:
		return fixedpoint.Zero, fmt.Errorf("unknown side: %s", side)
	}
}

// cancel order
func (o *TWAPExecutor) CancelOrder(ctx context.Context, order types.Order) error {
	timedCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	return o.executor.CancelOrders(timedCtx, order)
}
