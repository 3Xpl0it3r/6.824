package raft

import (
	"fmt"
	"time"
)

// AppendEntriesArgs represent appendentriesargs
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediatly preceding new ones

	PrevLogTerm int        // term of prevLogIndex entry
	Entries     []LogEntry // log entries to store (empty for heartbeat ,may send more than one for effictive)

	LeaderCommit int // leader's commitIndex

}

// AppendEntriesReply represent appendentriesreply
type AppendEntriesReply struct {
	Term      int  // current term for leader to update itself
	Success   bool // true if follower contained entry matching preLogIndex and prevlogTerm
	NextIndex int  // for performance
}

// Raft represent raft
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DebugPretty(dLog, "S%d ready AppEnt <- S%d (PLI:%d PLT:%d, LCMI:%d, LT:%d) - %d", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Term, time.Now().UnixMilli())

	rf.mu.Lock()
	DebugPretty(dLog, "S%d received AppEnt <- S%d (PLI:%d PLT:%d, LCMI:%d, LT:%d) - %d", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Term, time.Now().UnixMilli())
	reply.Success = false

	defer func() {
		reply.Term = rf.currentTerm
		rf.persist()
		rf.mu.Unlock()
	}()

	if !rf.validateTerm(args.Term, false, true) {
		DebugPretty(dLog, "S%d <- S:%d AppEnt failed - %v - %v", rf.me, args.LeaderId, time.Now().Sub(rf.termAt).Milliseconds(), rf.termAt.UnixMilli())
		return
	}

	rf.switchState(Any, Follower, func() {
		rf.resetElectionTimer()
	})

	if rf.commitIndex < rf.lastIncludedIndex {
		reply.NextIndex = rf.lastIncludedIndex + 1
	} else {
		reply.NextIndex = rf.commitIndex + 1
	}

	if rf.commitIndex > args.PrevLogIndex {
		err := fmt.Errorf("CMI:%d > PLI:%d(for commited log cannot be deleted)", rf.commitIndex, args.PrevLogIndex)
		DebugPretty(dLog, "S%d <- S:%d AppEnt failed - %v", rf.me, args.LeaderId, err)
		return
	}

	if ok := rf.findFollowerNextIndex(args); !ok {
		err := fmt.Errorf("notice Leader update nextIndex=%d", reply.NextIndex)
		DebugPretty(dLog, "S%d <- S:%d AppEnt failed - %v", rf.me, args.LeaderId, err)
		return
	}

	fixedIndex := args.PrevLogIndex - rf.lastIncludedIndex

	// findFollerNextIndex make sure that fixedIndex > 0, and fixedIndex in [0, len(rf.logs))
	rf.logs = rf.logs[:fixedIndex]
	rf.logs = append(rf.logs, args.Entries...)

	DebugPretty(dLog, "S%d ->S%d Saved Logs[%d](PLI:%v PLT:%v) [LI:%d CI:%d] at:T%d  all:%v - %v", rf.me, args.LeaderId, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, rf.lastApplied, rf.commitIndex, reply.Term, len(rf.logs), time.Now().UnixMilli())

	reply.Success = true
	rf.updateFollowerCommitIndex(args)

	DebugPretty(dLog, "S%d Exit receive AppEnt <- S%d (CMI:%d, lastAplied:%d lastIncludeIndex:%d ) - %d", rf.me, args.LeaderId, rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex, time.Now().UnixMilli())
}

// StartAppendEntries issue an new AppendLogEntriesRPC witout raft state machined lock
func (rf *Raft) StartAppendEntries() {
	rf.bastOkCnt = 1
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      []LogEntry{}, // for empty appendtry , if log is not empty, then update it
		LeaderCommit: rf.commitIndex,
	}

	rf.persist() // 2C

	logCopy := make([]LogEntry, len(rf.logs))
	copy(logCopy, rf.logs)
	DebugPretty(dLeader, "S%d BroadCast AppEnt T:%d MI:%v NI:%v LII:%d, CMI:%d len(log):%v - %v", rf.me, rf.currentTerm, rf.matchIndex, rf.nextIndex, rf.lastIncludedIndex, rf.commitIndex, len(rf.logs), time.Now().UnixMilli())
	for serverIdx := range rf.peers {
		if serverIdx == rf.me {
			continue
		}

		args.PrevLogIndex = rf.nextIndex[serverIdx] - 1

		// If args.PrevLogIndex < rf.lastIncludedIndex it means there some logs that will be send to follower are located at snapshots,
		// In this case we should send leader's snapshot to follower first;
		if args.PrevLogIndex < rf.lastIncludedIndex {
			// send issue append rpcs
			go rf.IssueInstallSnapshotRPC(serverIdx, SnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Offset:            0,
				Data:              rf.persister.ReadSnapshot(),
				Done:              true,
			})
			continue
		}

		// args.PrevlogIndex >= rf.lastIncludeIndex
		if args.PrevLogIndex == rf.lastIncludedIndex || len(rf.logs) == 0 {
			args.PrevLogTerm = rf.lastIncludedTerm
			args.Entries = logCopy
		} else { // len(rf.log) > 0 && args.PrevLogIndex > rf.lastIncldueIndex (args.PrevLoIndex <= rf.lastIncludeIndex + rf.log.len)
			args.Entries = logCopy[args.PrevLogIndex-rf.lastIncludedIndex:]
			args.PrevLogTerm = logCopy[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
		}

		go rf.IssueRequestAppendEntries(serverIdx, args)
	}
	DebugPretty(dLeader, "S%d broadcast complete", rf.me)

}

// Raft represent raft
func (rf *Raft) IssueRequestAppendEntries(server int, args AppendEntriesArgs) {
	reply := &AppendEntriesReply{}

	DebugPretty(dLog, "S%d -> S%d Send (PLI:%d PLT:%d LC:%d LT:%d) Ent:%v %v", rf.me, server, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Term, args.Entries, time.Now().UnixMilli())

	if rf.sendAppendEntries(server, &args, reply) {
		rf.handleBroadcastResponse(server, &args, reply)
	}

}

// Raft represent raft
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

// handleBroadcastResponse [#TODO](should add some comments)
func (rf *Raft) handleBroadcastResponse(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.validateTerm(reply.Term, false, false) {
		return
	}

	if reply.Success {

		if rf.matchIndex[server] > args.PrevLogIndex+len(args.Entries) {
			DebugPretty(dLog, "S%d -> S%d failed, matchIndex %d  < %d,  %v", rf.me, server, rf.matchIndex[server], args.PrevLogIndex+len(args.Entries), time.Now().UnixMilli())
			return
		}
		if rf.nextIndex[server] != args.PrevLogIndex+1 {
			DebugPretty(dLog, "S%d -> S%d failed, nextIndex: %d < %d, %v", rf.me, server, rf.nextIndex[server], args.PrevLogIndex+1, time.Now().UnixMilli())
			return
		}

		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] += len(args.Entries)
		DebugPretty(dLeader, "S%d -> S%d ok, update matchIndex %d nextIdx:%d (PLI:%d len(entries):%d) at:%v", rf.me, server, rf.matchIndex[server], rf.nextIndex[server], args.PrevLogIndex, len(args.Entries), time.Now().UnixMilli())
	} else {
		rf.nextIndex[server] = reply.NextIndex
		DebugPretty(dLeader, "S%d -> S%d ✘, set nextIndex %d, at:%v", rf.me, server, rf.nextIndex[server], time.Now().UnixMilli())
	}

}

// Raft represent raft
func (rf *Raft) updateLeaderCommitIndex() {
	rf.mu.Lock()
	if len(rf.logs) == 0 {
		rf.mu.Unlock()
		return
	}

	// DebugPretty(dInfo, "S%d Begin Update CMI: LII:%d CMI:%d len(log)=%d, MatchIndex:%v | log:%v,  CT:%d", rf.me, rf.lastIncludedIndex, rf.commitIndex, len(rf.logs), rf.matchIndex, rf.logs, rf.currentTerm)

	var n int
	var flag = false
	for n = len(rf.logs) + rf.lastIncludedIndex; n > rf.commitIndex && n > rf.lastIncludedIndex; n-- {
		majority := 0
		for sverIdx := range rf.peers {
			if rf.matchIndex[sverIdx] >= n && rf.logs[n-rf.lastIncludedIndex-1].Term == rf.currentTerm {
				majority++
			}
			if majority > len(rf.peers)/2 {
				flag = true
				rf.commitIndex = n
				break
			}
		}
	}
	rf.mu.Unlock()
	if flag {
		DebugPretty(dCommit, "S%d Update CMI %d -> %d %v ", rf.me, rf.commitIndex, n, time.Now().UnixMilli())
	}
}

// Raft represent raft
func (rf *Raft) updateFollowerCommitIndex(args *AppendEntriesArgs) {
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logs)+rf.lastIncludedIndex {
			rf.commitIndex = len(rf.logs) + rf.lastIncludedIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		DebugPretty(dCommit, "S%d UpdateCMI (LCMI:%d CMI:%d LenLog:%d) ", rf.me, args.LeaderCommit, rf.commitIndex, len(rf.logs))
	}
}

// findFollowerNextIndex return flase if log doesn't contain an entry at prevLogIndex whoes term matches prevLogTerm
func (rf *Raft) findFollowerNextIndex(args *AppendEntriesArgs) bool {
	fixedIndex := args.PrevLogIndex - rf.lastIncludedIndex

	if fixedIndex < 0 {
		panic("this is not possible")
	}

	if fixedIndex == 0 { // args.PrevLogIndex
		if args.PrevLogTerm == rf.lastIncludedTerm {
			return true
		}
		panic("this is impossible")
	}

	if fixedIndex > len(rf.logs) {
		return false
	}

	if fixedIndex == len(rf.logs) {
		if rf.logs[fixedIndex-1].Term == args.PrevLogTerm {
			return true
		}
		return false
	}

	if rf.logs[fixedIndex-1].Term == args.PrevLogTerm {
		return true
	}

	return false
}
