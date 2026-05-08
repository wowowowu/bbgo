package signal

import (
	"context"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/indicator/v2"
	"github.com/c9s/bbgo/pkg/types"
)

var orderBookSignalMetrics = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "xmaker_order_book_signal",
		Help: "",
	}, []string{"symbol"})

func init() {
	prometheus.MustRegister(orderBookSignalMetrics)
}

type StreamBookSetter interface {
	SetStreamBook(book *types.StreamOrderBook)
}

type MarketTradeStreamSetter interface {
	SetMarketTradeStream(stream types.Stream)
}

type OrderBookBestPriceVolumeSignal struct {
	BaseProvider
	Logger

	RatioThreshold fixedpoint.Value `json:"ratioThreshold"`
	MinVolume      fixedpoint.Value `json:"minVolume"`
	MinQuoteVolume fixedpoint.Value `json:"minQuoteVolume"`
	MinDelta       fixedpoint.Value `json:"minDelta"`

	Window          int                       `json:"window"`
	SmoothingWindow int                       `json:"smoothingWindow"`
	SmoothingType   indicatorv2.SmoothingType `json:"smoothingType"`

	symbol string
	book   *types.StreamOrderBook

	bidVolumeIndicator types.Float64Calculator
	askVolumeIndicator types.Float64Calculator

	bidVolumeSeries *types.Float64Series
	askVolumeSeries *types.Float64Series
}

func (s *OrderBookBestPriceVolumeSignal) ID() string {
	return "orderBookBestPrice"
}

func (s *OrderBookBestPriceVolumeSignal) SetStreamBook(book *types.StreamOrderBook) {
	s.book = book
}

func (s *OrderBookBestPriceVolumeSignal) Bind(ctx context.Context, session *bbgo.ExchangeSession, symbol string) error {
	if s.book == nil {
		return errors.New("s.book can not be nil")
	}

	s.symbol = symbol
	orderBookSignalMetrics.WithLabelValues(s.symbol).Set(0.0)

	if s.Window > 0 {
		s.bidVolumeSeries = types.NewFloat64Series()
		s.bidVolumeIndicator = indicatorv2.NewSmoothedIndicator(s.bidVolumeSeries, s.Window, s.SmoothingWindow, s.SmoothingType)

		s.askVolumeSeries = types.NewFloat64Series()
		s.askVolumeIndicator = indicatorv2.NewSmoothedIndicator(s.askVolumeSeries, s.Window, s.SmoothingWindow, s.SmoothingType)
	}

	return nil
}

func (s *OrderBookBestPriceVolumeSignal) CalculateSignal(ctx context.Context) (float64, error) {
	bid, ask, ok := s.book.BestBidAndAsk()
	if !ok {
		return 0.0, nil
	}

	bidVolume := bid.Price.Mul(bid.Volume)
	askVolume := ask.Price.Mul(ask.Volume)

	if s.bidVolumeSeries != nil {
		s.bidVolumeSeries.PushAndEmit(bid.Price.Mul(bid.Volume).Float64())
		s.askVolumeSeries.PushAndEmit(ask.Price.Mul(ask.Volume).Float64())

		bidVolume = fixedpoint.NewFromFloat(s.bidVolumeIndicator.(types.Series).Index(0))
		askVolume = fixedpoint.NewFromFloat(s.askVolumeIndicator.(types.Series).Index(0))
	}

	// TODO: may use scale to define this
	sumVol := bidVolume.Add(askVolume)
	bidRatio := bidVolume.Div(sumVol)
	askRatio := askVolume.Div(sumVol)
	denominator := fixedpoint.One.Sub(s.RatioThreshold)
	signal := 0.0
	if bidVolume.Compare(s.MinVolume) < 0 && askVolume.Compare(s.MinVolume) < 0 {
		signal = 0.0
	} else if bidVolume.Sub(askVolume).Abs().Compare(s.MinDelta) < 0 {
		signal = 0.0
	} else if bidRatio.Compare(s.RatioThreshold) >= 0 {
		numerator := bidRatio.Sub(s.RatioThreshold)
		signal = numerator.Div(denominator).Float64()
	} else if askRatio.Compare(s.RatioThreshold) >= 0 {
		numerator := askRatio.Sub(s.RatioThreshold)
		signal = -numerator.Div(denominator).Float64()
	}

	s.logger.Infof("[OrderBookBestPriceVolumeSignal] %f bid/ask = %f/%f, bid ratio = %f, ratio threshold = %f",
		signal,
		bidVolume.Float64(),
		askVolume.Float64(),
		bidRatio.Float64(),
		s.RatioThreshold.Float64(),
	)

	orderBookSignalMetrics.WithLabelValues(s.symbol).Set(signal)
	return signal, nil
}
