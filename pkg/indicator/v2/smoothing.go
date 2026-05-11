package indicatorv2

import (
	"github.com/c9s/bbgo/pkg/types"
)

type SmoothingType string

const (
	SmoothingTypeEMA  SmoothingType = "ema"
	SmoothingTypeEWMA SmoothingType = "ewma"
	SmoothingTypeRMA  SmoothingType = "rma"
	SmoothingTypeSMA  SmoothingType = "sma"
)

// NewSmoothedIndicator creates a new indicator with optional smoothing.
// It first applies a MAX indicator with the given window, and then optionally applies
// a smoothing indicator (RMA or EMA/EWMA) if smoothingWindow is greater than zero.
func NewSmoothedIndicator(source types.Float64Source, window int, smoothingWindow int, smoothingType SmoothingType) types.Float64Calculator {
	var base types.Float64Source = source
	if window > 0 {
		base = MAX(source, window)
	}

	if smoothingWindow > 0 {
		switch smoothingType {
		case SmoothingTypeRMA:
			return RMA2(base, smoothingWindow, true)
		case SmoothingTypeSMA:
			return SMA(base, smoothingWindow)
		case SmoothingTypeEWMA, SmoothingTypeEMA:
			return EWMA2(base, smoothingWindow)
		default:
			return EWMA2(base, smoothingWindow)
		}
	}

	return base.(types.Float64Calculator)
}
