package bbgo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/c9s/bbgo/pkg/types"
)

func TestMaintenanceConfig_IsInMaintenance(t *testing.T) {
	now := time.Now()
	past := types.LooseFormatTime(now.Add(-time.Hour))
	future := types.LooseFormatTime(now.Add(time.Hour))

	tests := []struct {
		name        string
		maintenance *MaintenanceConfig
		expected    bool
	}{
		{
			name: "In maintenance",
			maintenance: &MaintenanceConfig{
				StartTime: &past,
				EndTime:   &future,
			},
			expected: true,
		},
		{
			name: "Before maintenance",
			maintenance: &MaintenanceConfig{
				StartTime: &future,
				EndTime:   &future, // Not realistic but fine for test
			},
			expected: false,
		},
		{
			name: "After maintenance",
			maintenance: &MaintenanceConfig{
				StartTime: &past,
				EndTime:   &past,
			},
			expected: false,
		},
		{
			name:        "Maintenance not configured (nil times)",
			maintenance: &MaintenanceConfig{},
			expected:    false,
		},
		{
			name:        "Maintenance config is nil",
			maintenance: nil,
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.maintenance.IsInMaintenance())

			session := &ExchangeSession{
				ExchangeSessionConfig: ExchangeSessionConfig{
					Maintenance: tt.maintenance,
				},
			}
			assert.Equal(t, tt.expected, session.IsInMaintenance())
		})
	}
}
