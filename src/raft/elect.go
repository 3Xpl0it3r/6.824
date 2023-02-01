package raft

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	VoteForNone = -1
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = false
	DebugPretty(dVote, "S%d <-  C%d RequestVote (LI:%d LT:%d T:%d)", rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm, args.Term)

	topHalf := func() error {
		if rf.currentTerm < args.Term {
			rf.switchState(Any, Follower, func() {
				rf.updateTerm(args.Term)
				rf.updateVoteFor(VoteForNone)
			})
		}

		if reply.Term > args.Term {
			return fmt.Errorf("term check failed, curT:%d > rpcT:%d", rf.currentTerm, args.Term)
		}

		reply.Term = rf.currentTerm

		if rf.voteFor != VoteForNone {
			return fmt.Errorf("already voteFor %d != %d", rf.voteFor, args.CandidateId)
		}
		return nil
	}

	if err := rf.funcWrapperWithStateProtect(topHalf, LevelRaftSM); err != nil {
		DebugPretty(dTrace, "S%d -> C%d RejectVote, msg: %v", rf.me, args.CandidateId, err)
		return
	}

	bottomHalf := func() error {
		lastLogIndex := len(rf.log)

		if lastLogIndex != 0 {
			if args.LastLogTerm < rf.log[lastLogIndex-1].Term {
				return fmt.Errorf("cmp LT, myLT:%d > rpcLT:%d", reply.Term, args.Term)
			}

			if args.LastLogTerm == rf.log[lastLogIndex-1].Term && lastLogIndex > args.LastLogIndex {
				return fmt.Errorf("LT same, cmp LI, myLI:%d > rpcLI:%d", lastLogIndex, args.LastLogIndex)
			}
		}
		return nil
	}

	if err := rf.funcWrapperWithStateProtect(bottomHalf, LevelLogSS); err != nil {
		DebugPretty(dTrace, "S%d -> C%d RejectVote, msg: %v", err)
		return
	}

	//  updated status according to the result vote
	if err := rf.funcWrapperWithStateProtect(func() error {
		if rf.currentTerm == reply.Term {
			reply.VoteGranted = true
			rf.updateVoteFor(args.CandidateId)
			rf.switchState(Any, Follower, nil)
			return nil
		}
		return fmt.Errorf("drop myself, may rpc is out-of-date curT:%d != prevT:%d", rf.currentTerm, reply.Term)
	}, LevelRaftSM); err != nil {
		DebugPretty(dTrace, "S%d -> S%d RejectVote, msg: %v", rf.me, args.CandidateId, err.Error())
	} else {
		DebugPretty(dVote, "S%d -> S%d GrantVote", rf.me, args.CandidateId)
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
	return ok
}

// Raft represent raft
func (rf *Raft) ResponseVote(vote chan struct{}, voteCnt *int32, ctx context.Context, server int, args *RequestVoteArgs) {
	rf.funcWrapperWithStateProtect(func() error {
		DebugPretty(dVote, "S%d send vote(LI: %d LT: %d) -> S%d at: T%d(T%d)", rf.me, args.LastLogIndex, args.LastLogTerm, server, args.Term, rf.currentTerm)
		return nil
	}, LevelRaftSM)

	ackFn := func() {
		select {
		case <-ctx.Done():
			return
		case <-vote:
			return
		case vote <- struct{}{}:
		}
	}

	reply := &RequestVoteReply{}
	if !rf.sendRequestVote(server, args, reply) {
		DebugPretty(dError, "S%d -> S%d req vote net failed", rf.me, server)
		return
	}

	if err := rf.funcWrapperWithStateProtect(func() error {
		// Once found outself term is smalller than the term that rpc returned, we should immediately convert to follower without any  questions
		// So where we should pass `Any` but not `Candidate` to switchState()
		if reply.Term > rf.currentTerm {
			rf.switchState(Any, Follower, func() {
				rf.updateTerm(reply.Term)
				rf.updateVoteFor(VoteForNone)
			})
			return fmt.Errorf("chk term failed, curT:%d < rpcT:%d ", rf.currentTerm, reply.Term)
		}

		return nil
	}, LevelRaftSM); err != nil {
		DebugPretty(dVote, "S%d -> S%d req vote failed, msg: %s", rf.me, server, err.Error())
		return
	}

	DebugPretty(dVote, "S%d -> S%d req vote, got response %v", rf.me, server, reply.VoteGranted)

	if reply.VoteGranted {
		atomic.AddInt32(voteCnt, 1)
	}
	if atomic.LoadInt32(voteCnt) > int32(len(rf.peers)/2) {
		ackFn()
	}

}

// StartElection issue an vote rpc to all server in concurrency for performance when the server translation into candidate
func (rf *Raft) StartElection(term int, elt time.Duration) {
	var (
		voteCnt         int32         = 1
		voteCh          chan struct{} = make(chan struct{})
		timeout, cancel               = context.WithTimeout(context.Background(), elt)
	)

	concurVoteFn := func() error {
		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log),
			LastLogTerm:  0,
		}
		if len(rf.log) != 0 {
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
		}

		for servIdex := range rf.peers {
			if servIdex != rf.me {
				go rf.ResponseVote(voteCh, &voteCnt, timeout, servIdex, &args)
			}
		}
		return nil
	}

	rf.funcWrapperWithStateProtect(concurVoteFn, LevelLogSS)

	select {
	case <-timeout.Done():
		close(voteCh)
		return
	case <-voteCh:
	}
	cancel()

	rf.funcWrapperWithStateProtect(func() error {
		return rf.switchState(Candidate, Leader, func() {
			rf.funcWrapperWithStateProtect(rf.reInitializeVolatitleState, LevelLogSS)
			rf.StartAppendEntries(rf.currentTerm)
		})
	}, LevelRaftSM)
}

// reInitializeVolatitleState represent reinitializevolatitlestate
func (rf *Raft) reInitializeVolatitleState() error {
	for sverIdex := range rf.peers {
		rf.matchIndex[sverIdex] = 0
		rf.nextIndex[sverIdex] = 1
	}
	rf.matchIndex[rf.me] = len(rf.log)
	rf.nextIndex[rf.me] = len(rf.log) + 1
	return nil
}
