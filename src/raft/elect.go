package raft

import (
	"fmt"
)

const (
	VoteForNone = -1
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()

	defer func() {
		reply.Term = rf.currentTerm
		rf.persist()
		rf.mu.Unlock()
	}()

	reply.VoteGranted = false
	// DebugPretty(dVote, "S%d <- C%d Ask Vote (LI:%d LT:%d T:%d)", rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm, args.Term)

	if !rf.validateTerm(args.Term, true, true) {
		DebugPretty(dVote, "S%d <- C%d, reject (LI: %d, LT:%d T:%d)", rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm, args.Term)
		return
	}

	if rf.voteFor != VoteForNone {
		DebugPretty(dVote, "S%d <- C%d, reject, has vote to %d", rf.me, args.CandidateId, rf.voteFor)
		return
	}

	if err := rf.validateCandiatesLogIsUpdate(args); err != nil {
		DebugPretty(dVote, "S%d <- C%d, reject, %v", rf.me, args.CandidateId, err.Error())
		return
	}

	rf.switchState(Any, Follower, func() {
		reply.VoteGranted = true
		rf.updateVoteFor(args.CandidateId)
		rf.resetElectionTimer()

	})

	DebugPretty(dVote, "S%d -> C%d GrantVote %v newTerm:%v", rf.me, args.CandidateId, reply.VoteGranted, rf.termAt.UnixMilli())

}

// StartElection issue an vote rpc to all server in concurrency for performance when the server translation into candidate
func (rf *Raft) StartElection() {
	rf.updateTerm(rf.currentTerm + 1)
	rf.updateVoteFor(rf.me)
	rf.voteOkCnt = 1

	rf.persist()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) + rf.lastIncludedIndex,
		LastLogTerm:  0,
	}

	if len(rf.logs) != 0 {
		args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	} else {
		args.LastLogTerm = rf.lastIncludedTerm
	}

	for servIdex := range rf.peers {
		if servIdex != rf.me {
			go rf.IssueRequestVote(servIdex, &args)
		}
	}
	DebugPretty(dVote, "S%d end election ", rf.me)
}

// Raft represent raft
func (rf *Raft) IssueRequestVote(server int, args *RequestVoteArgs) {

	DebugPretty(dVote, "S%d ->S%d issue vote(LI: %d LT: %d) at: T%d", rf.me, server, args.LastLogIndex, args.LastLogTerm, args.Term)

	reply := &RequestVoteReply{}
	if rf.sendRequestVote(server, args, reply) {
		rf.handleVoteResponse(server, reply)
	}

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// handleVoteResponse [#TODO](should add some comments)
func (rf *Raft) handleVoteResponse(server int, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.validateTerm(reply.Term, false, false) == false {
		return
	}

	if reply.VoteGranted {
		rf.voteOkCnt += 1
		if rf.voteOkCnt > int(len(rf.peers)/2) && rf.role != Leader {
			DebugPretty(dVote, "S%d Win,Cvt to leader ", rf.me)
			rf.switchState(Candidate, Leader, rf.reInitializeVolatitleState)
		}
	}

}

// reInitializeVolatitleState represent reinitializevolatitlestate
func (rf *Raft) reInitializeVolatitleState() {
	for sverIdex := range rf.peers {
		rf.matchIndex[sverIdex] = 0
		rf.nextIndex[sverIdex] = len(rf.logs) + 1 + rf.lastIncludedIndex
	}
	rf.matchIndex[rf.me] = len(rf.logs) + rf.lastIncludedIndex
}

// logCompare [#TODO](should add some comments)
func (rf *Raft) validateCandiatesLogIsUpdate(args *RequestVoteArgs) error {
	lastLogIndex := len(rf.logs) + rf.lastIncludedIndex
	lastLogTerm := rf.lastIncludedTerm

	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	if lastLogIndex != 0 {
		// if args.LastLogTerm < rf.logs[lastLogIndex-1].Term {
		if args.LastLogTerm < lastLogTerm { // candidate's log lastLogTerm is smaller than myself's lastLogTerm
			// rf.persist() // 需要持久化么?
			return fmt.Errorf("cmp LT, myLT:%d > rpcLT:%d", lastLogTerm, args.LastLogTerm)
		}

		if args.LastLogTerm == lastLogTerm && lastLogIndex > args.LastLogIndex { // candidate's last log term is same as our last log's term ,but out log is logner, so candidate's log is not newer
			// rf.persist() // 需要持久化么？
			return fmt.Errorf("LT same, cmp LI, myLI:%d > rpcLI:%d", lastLogIndex, args.LastLogIndex)

		}
		// other case means candidate's log is newer than myself's log
	}
	return nil
}

// validateTerm [#TODO](should add some comments)
func (rf *Raft) validateTerm(term int, isVote, needPersist bool) bool {
	if rf.currentTerm < term {
		rf.switchState(Any, Follower, func() {
			rf.updateTerm(term)
			if isVote {
				rf.updateVoteFor(VoteForNone)
			}
		})
		return true
	}

	if rf.currentTerm > term {
		if needPersist {
			rf.persist()
		}
		return false
	}
	return true
}
