package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

const (
	midFileName    = "mr-mid-%d-%d"
	outputFileName = "mr-out-%d"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type TmpMidFile struct {
	file *os.File
	enc  *json.Encoder
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// send rpc to the coordinator, receive file path to start working
	var (
		rs        *GetTaskRS
		err       error
		allMidMap = make(map[int][]string)
	)
	for {
		rs, err = CallGetTask()
		if err != nil {
			fmt.Printf("Worker: GetMapTask Error: %v \n", err)
			break
		}

		// start exec task
		switch rs.TaskType {
		case MapTask:
			mapOneFile(rs.MapTask, rs.MapIndex, allMidMap, mapf, rs.ReduceCount)
		case ReduceTask:
			reduceOneFile(rs.ReduceIndex, allMidMap[rs.ReduceIndex], reducef)
		default:
			time.Sleep(3 * time.Second)
			fmt.Printf("Worker: Call GetMapTask task nil,keep 3 second waiting \n")
			continue
		}
	}

	fmt.Printf("Worker: Done \n")
}

func mapOneFile(path string, mapIndex int, allMidMap map[int][]string, mapf func(string, string) []KeyValue, reduceNum int) (midFileMap map[string]*TmpMidFile) {
	mapFile, err := os.Open(path)
	if err != nil {
		fmt.Printf("mapOneFile: Open Error: %v \n", err)
		return
	}
	defer func() {
		err = mapFile.Close()
		if err != nil {
			fmt.Printf("mapOneFile: Close Error: %v \n", err)
		}
	}()

	allBytes, err := io.ReadAll(mapFile)
	if err != nil {
		fmt.Printf("mapOneFile: ReadAll Error: %v \n", err)
		return
	}

	var (
		reduceIndex int
		tmpName     string
		tmpMid      *TmpMidFile
	)
	midFileMap = make(map[string]*TmpMidFile)

	result := mapf(path, string(allBytes))
	for _, KV := range result {
		reduceIndex = ihash(KV.Key) % reduceNum
		tmpName = fmt.Sprintf(midFileName, mapIndex, reduceIndex)

		tmpMid = midFileMap[tmpName]
		if tmpMid == nil {
			var tmpFile *os.File
			tmpFile, err = os.CreateTemp("", "tmp")
			if err != nil {
				fmt.Printf("mapOneFile: Open Error: %v \n", err)
				continue
			}

			tmpMid = &TmpMidFile{
				file: tmpFile,
				enc:  json.NewEncoder(tmpFile),
			}
			midFileMap[tmpName] = tmpMid

			allMidMap[reduceIndex] = append(allMidMap[reduceIndex], tmpName)
		}

		err = tmpMid.enc.Encode(&KV)
		if err != nil {
			fmt.Printf("mapOneFile: Encode Error: %v \n", err)
		}
	}

	for name, info := range midFileMap {
		err = os.Rename(info.file.Name(), name)
		if err != nil {
			fmt.Printf("mapOneFile: Rename Error: %v \n", err)
			return
		}

		err = info.file.Close()
		if err != nil {
			fmt.Printf("mapOneFile: Close Error: %v \n", err)
		}
	}

	go CallCompleteTask(MapTask, mapIndex)
	return
}

func reduceOneFile(reduceIndex int, allMidFiles []string, reducef func(string, []string) string) {
	if len(allMidFiles) == 0 {
		return
	}

	var (
		tmpFile *os.File
		err     error
		keyMap  = make(map[string][]string)
	)
	for _, name := range allMidFiles {
		if !strings.HasSuffix(name, fmt.Sprintf("-%d", reduceIndex)) {
			continue
		}

		tmpFile, err = os.Open(name)
		if err != nil {
			fmt.Printf("reduceOneFile: Open Error: %v \n", err)
			continue
		}

		dec := json.NewDecoder(tmpFile)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			}
			keyMap[kv.Key] = append(keyMap[kv.Key], kv.Value)
		}

		err = tmpFile.Close()
		if err != nil {
			fmt.Printf("reduceOneFile: Close Error: %v \n", err)
		}

		err = os.Remove(name)
		if err != nil {
			fmt.Printf("reduceOneFile: Remove Error: %v \n", err)
		}
	}

	tmpOutputFile, err := os.CreateTemp("", "tmp")
	if err != nil {
		fmt.Printf("reduceOneFile: Create Error: %v \n", err)
		return
	}

	for key, values := range keyMap {
		result := reducef(key, values)

		// this is the correct format for each line of Reduce output.
		_, err = fmt.Fprintf(tmpOutputFile, "%v %v\n", key, result)
		if err != nil {
			fmt.Printf("reduceOneFile: Write Error: %v \n", err)
			continue
		}
	}

	realName := fmt.Sprintf(outputFileName, reduceIndex)
	err = os.Rename(tmpOutputFile.Name(), realName)
	if err != nil {
		fmt.Printf("reduceOneFile: Rename Error: %v \n", err)
		return
	}

	err = tmpOutputFile.Close()
	if err != nil {
		fmt.Printf("reduceOneFile: Close Error: %v \n", err)
	}

	go CallCompleteTask(ReduceTask, reduceIndex)
}

func CallGetTask() (rs *GetTaskRS, err error) {
	rq := &GetTaskRQ{}

	rs = &GetTaskRS{}

	ok := call("Coordinator.GetTask", rq, rs)
	if !ok {
		fmt.Printf("call failed! \n")
		return nil, fmt.Errorf("call err")
	}

	if rs.ErrDesc != "" {
		return nil, fmt.Errorf(rs.ErrDesc)
	}

	fmt.Printf("GetTaskRS: %v\n", rs)
	return
}

func CallCompleteTask(taskType, index int) (rs *CompleteTaskRS) {
	rq := &CompleteTaskRQ{
		Type:  taskType,
		Index: index,
	}

	rs = &CompleteTaskRS{}

	for {
		ok := call("Coordinator.CompleteTask", rq, rs)
		if !ok {
			fmt.Printf("call failed! \n")
			time.Sleep(time.Millisecond * 100)
			continue
		}

		break
	}

	fmt.Printf("CompleteTask type:%d index:%d\n", taskType, index)
	return
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
