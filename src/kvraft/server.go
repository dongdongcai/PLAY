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

	table     map[string]string
	chanTable map[int]chan Op
	// Your definitions here.
}

//if is leader, return immediately, otherwise block until committed, may need a goroutine
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{Opname: "Get", Key: args.Key, Value: ""})
	kv.mu.Unlock()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.chanTable[index] = make(chan Op)
	if res := <-kv.chanTable[index]; res.Opname == "Get" && res.Key == args.Key {
		kv.mu.Lock()
		kv.mu.Unlock()
		reply.WrongLeader = false
		kv.mu.Lock()
		v, exist := kv.table[args.Key]
		kv.mu.Unlock()
		if exist {
			reply.Value = v
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		reply.WrongLeader = true
	}
	close(kv.chanTable[index])
	delete(kv.chanTable, index)
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{Opname: args.Op, Key: args.Key, Value: args.Value})
	kv.mu.Unlock()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.chanTable[index] = make(chan Op)
	if res := <-kv.chanTable[index]; res.Opname == args.Op && res.Key == args.Key && res.Value == args.Value {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		switch args.Op {
		case "Append":
			kv.table[args.Key] = kv.table[args.Key] + args.Value
		case "Put":
			kv.table[args.Key] = args.Value
		default:
			log.Fatal("This is impossible!")
		}
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
	close(kv.chanTable[index])
	delete(kv.chanTable, index)
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
	kv.chanTable = make(map[int]chan Op)

	//TODO: concern that write before read
	go func() {
		for {
			msg := <-kv.applyCh
			kv.chanTable[msg.Index] <- msg.Command.(Op)
		}
	}()
	// You may need initialization code here.

	return kv
}
