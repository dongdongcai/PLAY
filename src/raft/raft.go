package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	STATE_LEADER = iota
	STATE_CANDIDATE
	STATE_FOLLOWER

	HB_INTERVAL = time.Millisecond * 100
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term int
	Op   interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	currentTerm int
	voteFor     int
	state       int
	voteCount   int
	commitIndex int
	lastApplied int
	hbchan      chan bool
	elecchan    chan bool
	winner      chan bool
	commitchan  chan bool
	quit        chan bool
	log         []LogEntry
	nextIndex   []int
	matchIndex  []int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) initNextIndex() {
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = (rf.state == STATE_LEADER)
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.voteFor = -1
		rf.persist()
	}

	if (rf.getLastLogTerm() < args.LastLogTerm || rf.getLastLogTerm() == args.LastLogTerm && (len(rf.log)-1) <= args.LastLogIndex) && (rf.voteFor == -1 || rf.voteFor == args.CandidateId) {
		DPrintf("%d vote for %d, term(%d, %d)", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		rf.voteFor = args.CandidateId
		rf.persist()
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.elecchan <- true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			rf.voteFor = -1
			rf.persist()
			return ok
		} else if reply.VoteGranted {
			rf.voteCount++
			if rf.state == STATE_CANDIDATE && rf.voteCount > len(rf.peers)/2 {
				DPrintf("%d is now leader", rf.me)
				rf.state = STATE_LEADER
				rf.winner <- true
			}
		}
	}
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.hbchan <- true
	if len(args.Entries) > 0 {
		DPrintf("%d receive from leader %d, data is %v", rf.me, args.LeaderId, args)
	} else {
		DPrintf("%d receive hb from %d", rf.me, args.LeaderId)
	}
	if args.Term < rf.currentTerm || len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%d refuse leader %d, term(%d, %d), prevIndex(%d, %d)", rf.me, args.LeaderId, args.Term, rf.currentTerm, len(rf.log), args.PrevLogIndex)
		reply.Term = rf.currentTerm
		reply.Success = false
		if args.Term >= rf.currentTerm {
			if len(rf.log) <= args.PrevLogIndex {
				reply.NextIndex = len(rf.log)
				return
			}
			tmp := args.PrevLogIndex - 1
			for ; tmp > 0 && rf.log[tmp].Term != args.PrevLogTerm; tmp-- {
			}
			reply.NextIndex = tmp + 1
		}
		return
	} else {
		rf.voteFor = -1
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		reply.Success = true
		reply.NextIndex = args.PrevLogIndex + 1
		rf.persist()
	}

	for index, entry := range args.Entries {
		if len(rf.log) > args.PrevLogIndex+1+index && rf.log[args.PrevLogIndex+1+index].Term != entry.Term {
			rf.log = rf.log[:args.PrevLogIndex+1+index]
		}

		if len(rf.log) <= args.PrevLogIndex+1+index {
			rf.log = append(rf.log, entry)
		} else {
			rf.log[args.PrevLogIndex+1+index] = entry
		}
		rf.persist()
		reply.NextIndex++
	}

	if args.LeaderCommit > rf.commitIndex {
		if len(rf.log)-1 < args.LeaderCommit {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		DPrintf("%d advance to %d, log is %v", rf.me, rf.commitIndex, rf.log)
		rf.commitchan <- true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.voteFor = -1
			rf.state = STATE_FOLLOWER
			rf.currentTerm = reply.Term
			rf.persist()
		} else if reply.Success {
			rf.matchIndex[server] = reply.NextIndex - 1
			rf.nextIndex[server] = reply.NextIndex
		} else if rf.state == STATE_LEADER {
			//DPrintf("leader%d retry to %d, term(%d, %d)", args.LeaderId, server, rf.currentTerm)
			rf.nextIndex[server] = reply.NextIndex
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log[rf.nextIndex[server]:]
			go rf.sendAppendEntries(server, args, reply)
		}
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term, isLeader := rf.GetState()
	if isLeader {
		DPrintf("%d is leader, new log entry is %v", rf.me, command)
		rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
		rf.persist()
		DPrintf("leader log is update to %v", rf.log)
		rf.matchIndex[rf.me] = len(rf.log) - 1
		//go rf.replicationService()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.quit <- true
}

//TODO: combine heartBeatService and replicationService
func (rf *Raft) advanceCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	tmp := make([]int, len(rf.matchIndex))
	copy(tmp, rf.matchIndex)
	sort.Ints(tmp)
	DPrintf("%v", rf.matchIndex)
	N := tmp[len(tmp)/2]
	if rf.log[N].Term == rf.currentTerm && rf.commitIndex != N {
		rf.commitIndex = N
		DPrintf("leader %d commit to %d", rf.me, rf.commitIndex)
		rf.commitchan <- true
	}
}

func (rf *Raft) replicationService() {
	rf.advanceCommitIndex()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			var reply AppendEntriesReply
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].Term
			args.LeaderCommit = rf.commitIndex
			args.Entries = rf.log[rf.nextIndex[i]:]
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
}

func (rf *Raft) startElection() {
	DPrintf("%d start leader election", rf.me)
	rf.voteCount = 1
	for i := 0; i < len(rf.peers); i++ {
		if rf.state == STATE_CANDIDATE && i != rf.me {
			var reply RequestVoteReply
			args := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.getLastLogTerm()}
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}

func makeRandomNumber() int64 {
	seed := time.Now().UnixNano()
	source := rand.NewSource(seed)
	generator := rand.New(source)
	return generator.Int63n(300) + 300
}

//IMPORTANT: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	//log.SetOutput(os.Stdout)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.voteCount = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = STATE_FOLLOWER
	rf.elecchan = make(chan bool)
	rf.hbchan = make(chan bool)
	rf.winner = make(chan bool)
	rf.commitchan = make(chan bool)
	rf.quit = make(chan bool)
	rf.log = append(rf.log, LogEntry{0, 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.initNextIndex()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.state {
			case STATE_LEADER:
				select {
				case <-rf.quit:
					DPrintf("leader %d stop signal received", rf.me)
					return
				default:
					DPrintf("%d start hb", rf.me)
					go rf.replicationService()
					time.Sleep(HB_INTERVAL)
				}
			case STATE_FOLLOWER:
				select {
				case <-rf.hbchan:
				case <-rf.elecchan:
				case <-rf.quit:
					DPrintf("follower %d stop signal received", rf.me)
					return
				case <-time.After(time.Millisecond * (time.Duration(makeRandomNumber()))):
					DPrintf("%d timeout", rf.me)
					rf.state = STATE_CANDIDATE
				}
			case STATE_CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.voteFor = rf.me
				rf.persist()
				rf.mu.Unlock()
				go rf.startElection()
				select {
				case <-rf.quit:
					DPrintf("candidate %d stop signal received", rf.me)
					return
				case <-rf.hbchan:
					rf.state = STATE_FOLLOWER
				case <-rf.winner:
					rf.initNextIndex()
					//go rf.replicationService()
				case <-time.After(time.Millisecond * (time.Duration(makeRandomNumber()))):
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.commitchan:
				rf.mu.Lock()
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i].Op}
					applyCh <- msg
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			case <-rf.quit:
				return
			}
		}
	}()

	return rf
}
