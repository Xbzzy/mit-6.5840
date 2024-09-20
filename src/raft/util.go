package raft

import (
	"fmt"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		now := time.Now()
		fmt.Println(fmt.Sprintf("%s:%d]", now.Format("[2006-01-02 15:04:05"), now.UnixMilli()%1000), fmt.Sprintf(format, a...))
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
