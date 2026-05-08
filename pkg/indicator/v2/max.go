package indicatorv2

import (
	"github.com/c9s/bbgo/pkg/types"
)

// MAXStream calculates the maximum value in a window from a float number stream.
type MAXStream struct {
	*types.Float64Series

	window    int
	rawValues *types.Queue
}

func MAX(source types.Float64Source, window int) *MAXStream {
	s := &MAXStream{
		Float64Series: types.NewFloat64Series(),
		window:        window,
		rawValues:     types.NewQueue(window),
	}

	s.Bind(source, s)
	return s
}

func (s *MAXStream) Calculate(v float64) float64 {
	s.rawValues.Update(v)
	return types.Max(s.rawValues, s.window)
}

func (s *MAXStream) Truncate() {
	s.Slice = types.ShrinkSlice(s.Slice, MaxSliceSize, TruncateSize)
}
