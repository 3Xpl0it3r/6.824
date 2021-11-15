package raft

import (
	"context"
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}




func(rf *Raft)getServerState()ServerState{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.serverState
}


//case 1:receive AppendEntries RPC from current leader or granting vote to candidate,reset timer
//case 2:step into leader and start heartbeat,jump out of select section in ticker() to flush leader state
func (rf *Raft) resetTimer() {
	rf.lastRecordTime = time.Now()
	rand.Seed(time.Now().UnixNano())
	// for limit heartbeat will be triggered 10 times peer second, so the electionTimeout should be larger than 150-300ms
	rf.electionTimeout = time.Duration(rand.Int63n(200)+ 300) * time.Millisecond
}


func(rf *Raft)resetHeartBeat(){
	rf.lastRecordTime = time.Now()
}

// start goroutine attach an context
func(rf *Raft)startGoroutineProc(f func(ctx context.Context), ctx context.Context){
	rf.wg.Add(1)
	go f(ctx)
}
// wait for goroutine all existed/complete
func(rf *Raft)waitAllGoroutineComplete(){
	rf.wg.Wait()
}



// convertTo<State> is not concurrency safety, is should be locked by caller

// convert to follower,
func(rf *Raft)convertToFollower(newTerm int){
	rf.currentTerm = newTerm
	rf.serverState = Follower
	rf.voteFor = VoteForNone
}

// convert to leader
func(rf *Raft)convertToLeader(){
	rf.serverState = Leader
	// Figure2, Volatile state on leader ,Reinitialized after election
	for server, _ := range rf.peers{
		rf.nextIndex[server] = len(rf.log) + 1
		rf.matchIndex[server] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log)

}

// convert to candidate, when convert to candidate shoule inc current term, reset timer,
func(rf *Raft)convertToCandidate() {
	oldTerm := rf.currentTerm
	rf.serverState = Candidate
	rf.currentTerm ++
	rf.voteFor = rf.me
	rf.resetTimer()
	DebugPrint(dTimer, "S%d Cvt Candidate|RST ELT(ELT Timeout)| T%d -> T%d", rf.me, oldTerm,rf.currentTerm)
}


// Figure2.
func (rf *Raft)applyLogToStateMachine(){
	DebugPrint(dApply, "S%d Apply To StateMachine CMI %d LAI %d\n", rf.me, rf.commitIndex, rf.lastApplied)
	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied ++
		rf.applyCh <- ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[rf.lastApplied-1].Command,
			CommandIndex:  rf.lastApplied,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
	}
}

// Figure2. Rules for leader
func (rf *Raft)updateCommitIndex(){
	// if existed an N, such that N > commitIndex, a majority of matchIndex[i] >= N ,and log[N].Term == currentTerm;
	// set commitIndex = N

	for N := rf.commitIndex + 1; N <= len(rf.log) ; N ++ {
		var count = 0
		for _,mix := range rf.matchIndex {
			if mix >= N && rf.log[N-1].Term == rf.currentTerm{
				count ++
			}
			if count > len(rf.peers) >> 1 {
				prevCMI := rf.commitIndex
				rf.commitIndex = N
				DebugPrint(dCommit, "S%d CMI Updated %d -> %d",rf.me,  prevCMI, rf.commitIndex)
				break
			}
		}
	}
}



