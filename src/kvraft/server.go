package raftkv

import (
	"encoding/gob"
	"log"
	"sync"

	"6.824/src/labrpc"

	"6.824/src/raft"
)

var Debug = 1

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
	Opname   string
	Key      string
	Value    string
	ClientID int64
	OpNum    int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	table         map[string]string
	chanTable     map[int]chan Op
	clientRequest map[int64]int
	// Your definitions here.
}

func (kv *RaftKV) checkDup(id int64, opnum int) bool {
	if val, ok := kv.clientRequest[id]; ok {
		return (val >= opnum)
	}
	return false
}

//if is leader, return immediately, otherwise block until committed, may need a goroutine
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("Server %d:Getting %s", kv.me, args.Key)
	//kv.mu.Lock()
	DPrintf("Server %d lock:Getting %s", kv.me, args.Key)
	index, _, isLeader := kv.rf.Start(Op{Opname: "Get", Key: args.Key, Value: "", ClientID: args.ClientID, OpNum: args.OpNum})
	//kv.mu.Unlock()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	kv.chanTable[index] = make(chan Op, 100)
	kv.mu.Unlock()
	if res := <-kv.chanTable[index]; res.Opname == "Get" && res.Key == args.Key {
		DPrintf("Server %d final:Getting %s", kv.me, args.Key)
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
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("Server %d:%s %s to %s", kv.me, args.Op, args.Value, args.Key)
	//kv.mu.Lock()
	DPrintf("Server %d lock:%s %s to %s", kv.me, args.Op, args.Value, args.Key)
	index, _, isLeader := kv.rf.Start(Op{Opname: args.Op, Key: args.Key, Value: args.Value, ClientID: args.ClientID, OpNum: args.OpNum})
	//kv.mu.Unlock()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	kv.chanTable[index] = make(chan Op, 100)
	kv.mu.Unlock()
	if res := <-kv.chanTable[index]; res.Opname == args.Op && res.Key == args.Key && res.Value == args.Value {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
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

func (kv *RaftKV) apply(op Op) {
	if !kv.checkDup(op.ClientID, op.OpNum) {
		switch op.Opname {
		case "Append":
			DPrintf("Server %d final:Appending %s to %s", kv.me, op.Value, op.Key)
			kv.table[op.Key] = kv.table[op.Key] + op.Value
		case "Put":
			DPrintf("Server %d final:Putting %s to %s", kv.me, op.Value, op.Key)
			kv.table[op.Key] = op.Value
		default:
			//log.Fatal("This is impossible!")
		}
		kv.clientRequest[op.ClientID] = op.OpNum
	}
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
	kv.clientRequest = make(map[int64]int)

	go func() {
		for {
			msg := <-kv.applyCh
			//TODO: No one is receiving this except leader
			//TODO: Add request timeout
			kv.mu.Lock()
			tmp := msg.Command.(Op)
			DPrintf("%d receive msg back from RAFT, msg op is %s, msg key is %s, msg value is %s, locked", kv.me, tmp.Opname, tmp.Key, tmp.Value)
			kv.apply(tmp)
			if c, ok := kv.chanTable[msg.Index]; ok {
				c <- tmp
			}
			kv.mu.Unlock()
		}
	}()
	// You may need initialization code here.

	return kv
}
