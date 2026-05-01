package grid2

import (
	"time"
)


func getDateKey(ts time.Time) time.Time {
	dateKey := time.Date(
		ts.Year(), ts.Month(), ts.Day(),
		0, 0, 0, 0,
		ts.Location(),
	).UTC()
	return dateKey
}
