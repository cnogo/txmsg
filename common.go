package txmsg

import "time"

func GetMilliSecond(ts time.Time) int64 {
	return ts.UnixNano() / 1000
}

func GetMilliSecondBeforeNow(timeDiff time.Duration) int64 {
	return GetMilliSecond(time.Now().Add(-timeDiff))
}