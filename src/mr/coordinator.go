package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Ready = 0
	Doing = 1
	Done  = 2
)

type Coordinator struct {
	sync.RWMutex
	mapTask    []string
	mapStat    []int
	mapChecker map[int]*time.Timer

	reduceStat    []int
	reduceChecker map[int]*time.Timer
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(rq *GetTaskRQ, rs *GetTaskRS) (err error) {
	c.Lock()
	defer c.Unlock()

	rs.ReduceCount = len(c.reduceStat)

	var allMapDone = true
	for index, stat := range c.mapStat {
		if stat == Ready && rs.MapTask == "" {
			// find a ready task
			rs.MapTask = c.mapTask[index]
			rs.TaskType = MapTask
			rs.MapIndex = index
			c.mapStat[index] = Doing

			// have map task, check stat after 10s
			tmpTimer := time.AfterFunc(time.Second*10, func() {
				c.Lock()
				defer c.Unlock()

				if c.mapStat[rs.MapIndex] == Doing {
					// timeout
					c.mapStat[rs.MapIndex] = Ready
					fmt.Printf("map task %d timeout \n", rs.MapIndex)
				}
			})
			c.mapChecker[rs.MapIndex] = tmpTimer
			fmt.Printf("give map task:%d \n", rs.MapIndex)
		}

		if stat != Done {
			allMapDone = false
		}
	}

	if !allMapDone {
		return
	}

	for index, stat := range c.reduceStat {
		if stat == Ready {
			rs.ReduceIndex = index
			rs.TaskType = ReduceTask
			c.reduceStat[index] = Doing

			// have reduce task, check stat after 10s
			tmpTimer := time.AfterFunc(time.Second*10, func() {
				c.Lock()
				defer c.Unlock()

				if c.reduceStat[rs.ReduceIndex] == Doing {
					// timeout
					c.reduceStat[rs.ReduceIndex] = Ready
					fmt.Printf("reduce task %d timeout \n", rs.ReduceIndex)
				}
			})
			c.reduceChecker[rs.ReduceIndex] = tmpTimer
			fmt.Printf("give reduce task:%d \n", rs.ReduceIndex)
			return
		}
	}

	return
}

func (c *Coordinator) CompleteTask(rq *CompleteTaskRQ, rs *CompleteTaskRS) (err error) {
	c.Lock()
	defer c.Unlock()

	switch rq.Type {
	case MapTask:
		c.mapStat[rq.Index] = Done

		timer := c.mapChecker[rq.Index]
		if timer != nil {
			timer.Stop()
			delete(c.mapChecker, rq.Index)
		}
		fmt.Printf("map task %d complete \n", rq.Index)
	case ReduceTask:
		c.reduceStat[rq.Index] = Done

		timer := c.reduceChecker[rq.Index]
		if timer != nil {
			timer.Stop()
			delete(c.reduceChecker, rq.Index)
		}
		fmt.Printf("reduce task %d complete \n", rq.Index)
	default:

	}

	return
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.RLock()
	defer c.RUnlock()
	for _, stat := range c.reduceStat {
		if stat != Done {
			// there has outstanding task
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		mapTask:       files,
		mapStat:       make([]int, len(files)),
		mapChecker:    make(map[int]*time.Timer),
		reduceStat:    make([]int, nReduce),
		reduceChecker: make(map[int]*time.Timer),
	}

	c.server()
	return c
}
