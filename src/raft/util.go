package raft

import (
	"fmt"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		fmt.Println(time.Now().UnixMilli(), fmt.Sprintf(format, a...))
	}
}
