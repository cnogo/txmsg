package txmsg

import (
	"fmt"
	"time"
)

func GetMilliSecond(ts time.Time) int64 {
	return ts.UnixNano() / 1000
}

func GetMilliSecondBeforeNow(timeDiff time.Duration) int64 {
	return GetMilliSecond(time.Now().Add(-timeDiff))
}

func Recover(cleanups ...func()) {
	for _, cleanup := range cleanups {
		cleanup()
	}

	if p := recover(); p != nil {
		fmt.Println(p)
	}
}

func GoSafe(fn func()) {
	go RunSafe(fn)
}

func RunSafe(fn func()) {
	defer Recover()
	fn()
}

type TaskRunner struct {
	limitChan chan struct{}
}

func NewTaskRunner(limitCnt int) *TaskRunner {
	return &TaskRunner{
		limitChan: make(chan struct{}, limitCnt),
	}
}

func (rp *TaskRunner) Schedule(task func()) {
	rp.limitChan <- struct{}{}

	go func() {
		defer Recover(func() {
			<- rp.limitChan
		})
		task()
	}()
}