package raftkv

import (
	"encoding/gob"
	"log"
	"sync"

	"6.824/src/labrpc"

	"6.824/src/raft"
)

var Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opname string
	Key    string
	Value  string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	table map[string]string
	// Your definitions here.
}

//if is leader, return immediately, otherwise block until committed
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	_, _, isLeader := kv.rf.Start(Op{Opname: "Get", Key: args.Key, Value: ""})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, _, isLeader := kv.rf.Start(Op{Opname: args.Op, Key: args.Key, Value: args.Value})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.table = make(map[string]string)

	go func() {
		for {
			msg := <-kv.applyCh
			switch op := msg.Command.(Op); op.Opname {
			case "Get":
				kv.table[op.Key] = op.Value
			case "Put":
				kv.table[op.Key] = op.Value
			case "Append":
				kv.table[op.Key] = op.Value
			default:
				kv.table[op.Key] = op.Value
			}
		}
	}()
	// You may need initialization code here.

	return kv
}
