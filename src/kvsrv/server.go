package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	dataMap map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.dataMap[args.Key]
	return
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.dataMap[args.Key] = args.Value
	return
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.dataMap[args.Key]
	if !ok {
		kv.dataMap[args.Key] = args.Value
		return
	}

	reply.Value = val
	kv.dataMap[args.Key] += args.Value
	return
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.dataMap = make(map[string]string)

	return kv
}
