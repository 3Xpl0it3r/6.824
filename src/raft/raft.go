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
	//	"bytes"

	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	_ "net/http/pprof"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	defaultTickerPeriod        time.Duration = 10 * time.Millisecond
	defaultMinEelectionTimeout time.Duration = 150 * time.Millisecond
	defaultHeartbeatPeriod     time.Duration = 100 * time.Millisecond
)

type Role int32

const (
	Any Role = iota
	Leader
	Follower
	Candidate
)

// Role represent role
func (r Role) String() string {
	switch r {
	case Leader:
		return "leader"
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Any:
		return "any"
	default:
		panic("unknown server role %d")
	}
}

// Raft represent raft
func (rf *Raft) GetRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

// LogEntry represent logeentry
type LogEntry struct {
	Term    int
	Command interface{}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg

	// blew are protected by rf.mu
	currentTerm int // latest term server has seen (initialized to 0 first boot)
	voteFor     int //candidateId that received vote in current term(or null if none)
	voteOkCnt   int
	bastOkCnt   int

	role   Role
	termAt time.Time

	logMu sync.Mutex
	// fileds below are protected by logMu
	logs        []LogEntry //log entries ,each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	commitIndex int
	lastApplied int
	nextIndex   []int // for guess, used for performance
	matchIndex  []int // used for safety

	lastIncludedIndex int
	lastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.voteFor)
	encoder.Encode(rf.logs)
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.lastIncludedTerm)
	rf.persister.SaveRaftState(writer.Bytes())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var (
		currentTerm       int
		voteFor           int
		logs              []LogEntry
		lastIncludedIndex int
		lastIncludedTerm  int
	)
	if decoder.Decode(&currentTerm) != nil || decoder.Decode(&voteFor) != nil || decoder.Decode(&logs) != nil {
		panic("readPersist failed")
	}
	if decoder.Decode(&lastIncludedIndex) != nil || decoder.Decode(&lastIncludedTerm) != nil {
		panic("read Raft Status failed")
	}
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.logs = logs
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// te service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}

	leaderCheck := func() error {
		term = rf.currentTerm
		isLeader = rf.role == Leader
		if !isLeader {
			return errors.New("S%d I am not leader, Start failed")
		}
		return nil
	}

	addLog := func() error {
		rf.persist()
		rf.logs = append(rf.logs, LogEntry{
			Term:    term,
			Command: command,
		})
		index = len(rf.logs) + rf.lastIncludedIndex
		rf.matchIndex[rf.me]++
		rf.nextIndex[rf.me]++
		DebugPretty(dLog2, "S%d Start Append cmd %v at %d,all: %d - %d ", rf.me, command, index, len(rf.logs), time.Now().UnixMilli())
		return nil
	}

	rf.funcWrapperWithStateProtect(leaderCheck, addLog)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	// DebugPretty(dInfo, "S%d Offline T:%d", rf.me, rf.GetTerm())
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.cleanUp()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		switch rf.GetRole() {
		case Follower:
			rf.StartFollower()
		case Candidate:
			rf.StartCandidate()
		case Leader:
			rf.StartLeader()
		}
	}
}

// Raft represent raft
func (rf *Raft) StartFollower() {
	elt := randomizedElectionTimeout()

	rf.funcWrapperWithStateProtect(func() error {
		DebugPretty(dTimer, "S%d I'm follower at T%d, pausing HBT elt:%d termAt:[%v] now:%d", rf.me, rf.currentTerm, elt/time.Millisecond, rf.termAt.UnixMilli(), time.Now().UnixMilli())
		return nil
	})

	for !rf.elapseElectionTimeoutUntil(Follower, elt, func() { rf.switchState(Follower, Candidate, rf.resetElectionTimer) }) {
		rf.applyLogEntries()
		time.Sleep(defaultTickerPeriod)
	}

}

// StartCandidate start an new election in its own term
func (rf *Raft) StartCandidate() {
	elt := randomizedElectionTimeout()

	rf.funcWrapperWithStateProtect(func() error {
		DebugPretty(dTimer, "S%d Convert to Candidate -termAt:%d now:%d", rf.me, rf.termAt.UnixMilli(), time.Now().UnixMilli())
		rf.StartElection()
		return nil
	})

	for !rf.elapseElectionTimeoutUntil(Candidate, elt, rf.resetElectionTimer) {
		time.Sleep(defaultTickerPeriod)
	}

}

// StartLeader start an loop worker for issue an appendRPC every heartbeat period until it's not leader anymore
func (rf *Raft) StartLeader() {
	rf.funcWrapperWithStateProtect(func() error {
		rf.StartAppendEntries()
		return nil
	})

	for !rf.elapseElectionTimeoutUntil(Leader, defaultHeartbeatPeriod, rf.resetElectionTimer) {
		rf.updateLeaderCommitIndex()
		rf.applyLogEntries()
		time.Sleep(defaultTickerPeriod)
	}
}

// Raft represent raft
// true means it can skip current loop, step into next role
func (rf *Raft) elapseElectionTimeoutUntil(role Role, elt time.Duration, breakFn func()) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != role || time.Now().Sub(rf.termAt)-elt > 0 {
		if breakFn != nil {
			breakFn()
		}
		return true
	}

	return false
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.initialization()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// go rf.applyLogToStateMachine(rf.stopCh)

	return rf
}

// Raft represent raft
func (rf *Raft) initialization() {
	rf.applyCh = make(chan ApplyMsg)
	rf.role = Follower
	rf.termAt = time.Now()

	rf.currentTerm = 0
	rf.voteFor = VoteForNone

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for sverIdex := range rf.peers {
		rf.nextIndex[sverIdex] = len(rf.logs) + 1
		rf.matchIndex[sverIdex] = 0
	}
	rf.matchIndex[rf.me] = len(rf.logs)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1

}

// Raft represent raft
func (rf *Raft) cleanUp() {
}
