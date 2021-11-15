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
		wg       *sync.WaitGroup = &sync.WaitGroup{}
		voteNums int32           = 0
	)
	defer func() {
		rf.wg.Done()
	}()
	select {
	case <-rf.ctx.Done():
		return
	default:
	}
	rf.mu.Lock()

	if rf.serverState == Leader {
		rf.mu.Unlock()
		return
	}
	rf.convertToCandidate()
	voteNums += 1
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: len(rf.log),
	}
	if len(rf.log) == 0{
		args.LastLogTerm = -1
	}else {
		args.LastLogTerm = rf.log[args.LastLogIndex-1].Term
	}

	rf.mu.Unlock()
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		wg.Add(1)
		go rf.wrapSendRequestVote(wg, &voteNums, server, &args)
	}
	wg.Wait()

}


func (rf *Raft) wrapSendRequestVote(wg *sync.WaitGroup, voteNums *int32, server int, args *RequestVoteArgs) {
	defer func() {
		wg.Done()
	}()
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()


	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		DebugPrint(dError, "S%d -> S%d, VoteRPC(NetErr)/T%d",rf.me, server,currentTerm)
	}

	if reply.VoteGranted == false {
		if reply.Term > rf.currentTerm {
			DebugPrint(dWarn, "S%d -> S%d, VoteRPC(Cvt Follower), For(T%d -> T%d)", rf.me, server, rf.currentTerm, reply.Term)
			rf.convertToFollower(reply.Term)
		}
	} else {
		atomic.AddInt32(voteNums, 1)
		DebugPrint(dVote, "S%d -> S%d, VoteRPC(Got Vote)/T%d", rf.me, server, rf.currentTerm)
		if atomic.LoadInt32(voteNums) > int32(len(rf.peers)>>1) {
			DebugPrint(dLeader, "S%d, VoteRPC(Archived Majority [%d])/T%d, converting to Leader", rf.me, atomic.LoadInt32(voteNums), rf.currentTerm)
			rf.convertToLeader()
		}
	}

}
