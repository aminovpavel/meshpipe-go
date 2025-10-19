package storage

import "time"

func secondsToTime(value float64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	sec := int64(value)
	nsec := int64((value - float64(sec)) * 1e9)
	return time.Unix(sec, nsec).UTC()
}

func timeToSeconds(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(t.UnixNano()) / 1e9
}
