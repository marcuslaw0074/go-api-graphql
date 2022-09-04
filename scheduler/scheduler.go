package scheduler

import (
	"fmt"
	"time"
)

func TestFunction(sleep int64) {
	var sleepTotal int64 = 0
	for {
		time.Sleep(time.Duration(sleep))
		sleepTotal = sleepTotal+ sleep
		fmt.Println(sleepTotal)
	}
}


