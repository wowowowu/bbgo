package indicatorv2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/c9s/bbgo/pkg/types"
)

func TestMAX(t *testing.T) {
	source := types.NewFloat64Series()
	maxIndicator := MAX(source, 3)

	// Test case 1: Fill the window
	source.PushAndEmit(1.0)
	assert.Equal(t, 1.0, maxIndicator.Last(0))

	source.PushAndEmit(3.0)
	assert.Equal(t, 3.0, maxIndicator.Last(0))

	source.PushAndEmit(2.0)
	assert.Equal(t, 3.0, maxIndicator.Last(0))

	// Test case 2: Sliding window
	source.PushAndEmit(1.0)
	// Window is [3.0, 2.0, 1.0], max is 3.0
	assert.Equal(t, 3.0, maxIndicator.Last(0))

	source.PushAndEmit(0.5)
	// Window is [2.0, 1.0, 0.5], max is 2.0
	assert.Equal(t, 2.0, maxIndicator.Last(0))

	source.PushAndEmit(4.0)
	// Window is [1.0, 0.5, 4.0], max is 4.0
	assert.Equal(t, 4.0, maxIndicator.Last(0))
}
