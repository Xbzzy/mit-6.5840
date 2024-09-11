package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type GetTaskRQ struct {
}

type GetTaskRS struct {
	ErrDesc     string
	ReduceCount int
	TaskType    int
	MapIndex    int
	MapTask     string
	ReduceIndex int
}

const (
	MapTask    = 1
	ReduceTask = 2
)

type CompleteTaskRQ struct {
	Type  int
	Index int
}

type CompleteTaskRS struct {
	ErrDesc string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
