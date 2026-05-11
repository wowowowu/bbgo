package bbgo

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceConfig_UnmarshalJSON(t *testing.T) {
	jsonData := `{
		"startTime": "yesterday",
		"endTime": "now",
		"balanceQueryAvailable": true
	}`

	var config MaintenanceConfig
	err := json.Unmarshal([]byte(jsonData), &config)
	require.NoError(t, err)

	assert.True(t, config.BalanceQueryAvailable)
	require.NotNil(t, config.StartTime)
	require.NotNil(t, config.EndTime)

	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)

	// Since "now" and "yesterday" are relative to when UnmarshalJSON was called,
	// they should be very close to the times we just calculated.
	assert.WithinDuration(t, yesterday, config.StartTime.Time(), 10*time.Second)
	assert.WithinDuration(t, now, config.EndTime.Time(), 10*time.Second)
}
