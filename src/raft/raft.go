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

	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	default:
		panic("unknown server role")
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

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
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
	role        Role
	termAt      time.Time

	logMu sync.Mutex
	// fileds below are protected by logMu
	log         []LogEntry //log entries ,each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	commitIndex int
	lastApplied int
	nextIndex   []int // for guess, used for performance
	matchIndex  []int // used for safety

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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}

	if err := rf.funcWrapperWithStateProtect(func() error {
		term = rf.currentTerm
		isLeader = rf.role == Leader
		if !isLeader {
			return errors.New("not leader")
		}
		return nil
	}, LevelRaftSM); err != nil {
		return index, term, false
	}

	rf.funcWrapperWithStateProtect(func() error {
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Command: command,
		})
		index = len(rf.log)
		rf.matchIndex[rf.me]++
		rf.nextIndex[rf.me]++
		DebugPretty(dLog2, "S%d Start Append cmd %v,all:%v", rf.me, command, rf.log)
		return nil
	}, LevelLogSS)

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
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
		elt := randomizedElectionTimeout()
		switch rf.GetRole() {
		case Follower:
			rf.StartFollower(elt)
		case Candidate:
			rf.StartCandidate(elt)
		case Leader:
			rf.StartLeader()
		}

		rf.funcWrapperWithStateProtect(func() error {
			rf.resetElectionTimer()
			return nil
		}, LevelRaftSM)
	}
}

// Raft represent raft
func (rf *Raft) StartFollower(elt time.Duration) {
	// this is should be delete, only to print some atomic values for ds debug
	rf.funcWrapperWithStateProtect(func() error {
		DebugPretty(dTimer, "S%d I'm follower at T%d, pausing HB %d [%v]", rf.me, rf.currentTerm, elt/time.Millisecond, time.Now().UnixMilli())
		return nil
	}, LevelRaftSM)

	time.Sleep(elt)
	if !rf.checkElectionTimeout(elt) {
		return
	}

	// this function will cause some atomic value changes, so this shoule be wrapper with protect function
	rf.funcWrapperWithStateProtect(func() error {
		if err := rf.switchState(Follower, Candidate, nil); err!=nil {
			panic(fmt.Errorf("S%d switchState expected %s but got %s", rf.me, Follower.String(), rf.role.String()))
		}
		return nil
	}, LevelRaftSM)
}

// StartCandidate start an new election in its own term
func (rf *Raft) StartCandidate(elt time.Duration) {
	rf.funcWrapperWithStateProtect(func() error {
		DebugPretty(dTimer, "S%d Cvert Candidate, calling vote T%d [%v]", rf.me, rf.currentTerm, time.Now().UnixMilli())
		if err := rf.switchState(Candidate, Candidate, func() {
			rf.resetElectionTimer()
			rf.updateTerm(rf.currentTerm + 1)
			rf.updateVoteFor(rf.me)
		}); err != nil {
			panic(fmt.Errorf("S%d switchState expected %s but got %s", rf.me, Candidate.String(), rf.role.String()))
		}
		go rf.StartElection(rf.currentTerm, elt)
		return nil
	}, LevelRaftSM)

	for rf.GetRole() == Candidate {
		if rf.checkElectionTimeout(elt) {
			return
		}
		time.Sleep(defaultTickerPeriod)
	}
}

// StartLeader start an loop worker for issue an appendRPC every heartbeat period until it's not leader anymore
func (rf *Raft) StartLeader() {
	var term int
	rf.funcWrapperWithStateProtect(func() error {
		term = rf.currentTerm
		go rf.StartAppendEntries(term)
		return nil
	}, LevelRaftSM)

	for rf.GetRole() == Leader {
		if rf.checkElectionTimeout(defaultHeartbeatPeriod) {
			DebugPretty(dInfo, "S%d issue appent rpc for timeout", rf.me)
			break
		}

		if err := rf.funcWrapperWithStateProtect(func() error {
			// if find an new commitindex which is larger than the old one ,then immediately send
			if rf.updateLeaderCommitIndex(term) {
				return errors.New("find n > commitIndex, need send")
			}

			rf.applyLogEntry()
			return nil

		}, LevelLogSS); err != nil {
			DebugPretty(dInfo, "S%d issue appent rpc for : %v", rf.me, err)
			break
		}

		time.Sleep(defaultTickerPeriod)
	}
}

// Raft represent raft
func (rf *Raft) checkElectionTimeout(elt time.Duration) bool {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()

	}()
	delay := time.Now().Sub(rf.termAt) - elt
	if delay > 0 {
		return true
	}
	return false
}

// Raft represent raft
func (rf *Raft) commitIndexChecker() bool {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	return rf.commitIndex < len(rf.log)
}

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
		rf.nextIndex[sverIdex] = len(rf.log) + 1
		rf.matchIndex[sverIdex] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log)

}

// Raft represent raft
func (rf *Raft) cleanUp() {
}
