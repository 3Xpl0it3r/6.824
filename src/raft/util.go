package raft

import (
	"context"
	"fmt"
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


func (rf *Raft)DPrintf(stage string, format string, a ...interface{})(n int,err error){
	format = fmt.Sprintf("%.24s Stag: %.24s\t Term: %d\tServer: %d\t State: %s\t%s\n", time.Now().String(),stage, rf.currentTerm, rf.me, rf.serverState, format)
	if Debug{
		fmt.Printf(format, a...)
	}
	return 0, err
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
}

// convert to candidate, when converto to candidate shoule inc current term, reset timer,
func(rf *Raft)convertToCandidate(){
	rf.currentTerm += 1
	rf.serverState = Candidate
	rf.voteFor = rf.me
	rf.resetTimer()
}