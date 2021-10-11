package raft

import (
	"context"
	"sync"
	"sync/atomic"
)

const (
	VoteForNone = -1
)

func (rf *Raft) doElection(ctx context.Context) {
	var (
		wg *sync.WaitGroup = &sync.WaitGroup{}
		voteNums int32 = 0
	)
	defer func() {
		rf.wg.Done()
	}()
	select {
	case <- rf.ctx.Done():
		return
	default:
	}
	rf.mu.Lock()
	if rf.serverState == Leader{
		rf.mu.Unlock()
		return
	}
	rf.convertToCandidate()
	voteNums+=1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
	}
	rf.mu.Unlock()
	for server, _ := range rf.peers{
		if server == rf.me{
			continue
		}
		wg.Add(1)
		go rf.wrapSendRequestVote(wg, &voteNums, server, &args)
	}
	wg.Wait()
}

func(rf *Raft)wrapSendRequestVote(wg *sync.WaitGroup, voteNums *int32,server int, args *RequestVoteArgs){
	defer func() {
		wg.Done()
		rf.mu.Lock()
		rf.mu.Unlock()
	}()
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		// todo log error
	}
	if reply.VoteGranted ==false{
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
		}
		rf.mu.Unlock()
	}else {
		atomic.AddInt32(voteNums, 1)
		rf.mu.Lock()
		if atomic.LoadInt32(voteNums) > int32(len(rf.peers) >> 1) {
			rf.convertToLeader()
			rf.heartbeatCh <- struct{}{}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) doHeartbeat(ctx context.Context) {
	var (
		wg *sync.WaitGroup = &sync.WaitGroup{}
	)
	defer func() {
		wg.Wait()
		rf.wg.Done()
	}()
	select {
	case <- rf.ctx.Done():
		return
	default:
	}
	rf.mu.Lock()
	if rf.serverState != Leader{
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      nil,
	}
	rf.mu.Unlock()
	for server,_ := range rf.peers{
		if server == rf.me{
			continue
		}
		wg.Add(1)
		go rf.wrapSendAppendEntries(wg, server, &args)
	}
}

func(rf *Raft)wrapSendAppendEntries(wg *sync.WaitGroup, server int, args *AppendEntriesArgs){
	defer func() {
		wg.Done()
	}()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		// todo log error
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.serverState != Leader{
		return
	}

	if reply.Term > rf.currentTerm{
		rf.convertToFollower(reply.Term)
	} else {
		// todo for lab2b,2c,2d
	}
}