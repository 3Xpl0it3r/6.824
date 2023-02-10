package raft

import (
	"context"
	"fmt"
	"sync/atomic"
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
	DebugPretty(dLog, "S%d receive AppEnt <- S%d (PLI:%d PLT:%d, LCMI:%d, LT:%d) - %d", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Term, time.Now().UnixMilli())

	termValidate := func() error {

		// if rpcs term is smaller than ourself's term ,then we shoud reconized this request is illegal, then drop it
		if rf.currentTerm > args.Term {
			err := fmt.Errorf("chk term faied, curT:%d > rpcT:%d", rf.currentTerm, reply.Term)
			reply.Term = rf.currentTerm
			return err
		}

		// if ourself term is smaller than rpc's term, then we should immediately translate to follower whatever role we now in
		rf.switchState(Any, Follower, func() {
			if rf.currentTerm < args.Term {
				rf.updateTerm(args.Term)
			}
		})

		rf.resetElectionTimer()
		DebugPretty(dTimer, "S%d <- S:%d AppEnt reset elt - %v - %v", rf.me, args.LeaderId, time.Now().Sub(rf.termAt).Milliseconds(), rf.termAt.UnixMilli())
		reply.Term = rf.currentTerm
		return nil
	}

	logHandler := func() error {
		reply.NextIndex = rf.commitIndex + 1

		if rf.commitIndex > args.PrevLogIndex {
			return fmt.Errorf("CMI:%d > PLI:%d(for commited log cannot be deleted)", rf.commitIndex, args.PrevLogIndex)
		}

		if ok := rf.findFollowerNextIndex(args); !ok {
			return fmt.Errorf("notice Leader update nextIndex=%d", reply.NextIndex)
		}

		// remove conflict log entries and then append new entries into local storage
		rf.log = rf.log[0:args.PrevLogIndex]
		rf.log = append(rf.log, args.Entries...)

		// update follower commit index and apply log to statemachine if necessary
		rf.updateFollowerCommitIndex(args)
		rf.applyLogEntry()

		DebugPretty(dLog, "S%d ->S%d Saved Logs[%d](PLI:%v PLT:%v) [LI:%d CI:%d] at:T%d  all:%v - %v", rf.me, args.LeaderId, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, rf.lastApplied, rf.commitIndex, reply.Term, len(rf.log), time.Now().UnixMilli())

		reply.Success = true
		return nil
	}

	rf.funcWrapperWithStateProtect(termValidate, logHandler, func() error { rf.persist(); return nil })
}

// Raft represent raft
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

// Raft represent raft
func (rf *Raft) IssueRequestAppendEntries(count *int32, server int, args AppendEntriesArgs) {
	reply := &AppendEntriesReply{}

	DebugPretty(dLog, "S%d -> S%d Send (PLI:%d PLT:%d LC:%d LT:%d) Ent:%v %v", rf.me, server, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Term, args.Entries, time.Now().UnixMilli())

	if !rf.sendAppendEntries(server, &args, reply) {
		return
	}

	// termValidate is used to validate this response is validate, or should drop it and stop handling
	termValidate := func() error {

		//
		if rf.currentTerm > args.Term {
			return fmt.Errorf("belated rpc request curT:%d < oldT:%d", rf.currentTerm, args.Term)
		}

		if rf.currentTerm < reply.Term {
			err := fmt.Errorf("ourself term invalid , ,cvt follower curT:%d -> rpcT:%d", rf.currentTerm, reply.Term)
			rf.switchState(Any, Follower, func() {
				rf.updateTerm(reply.Term)
			})
			return err
		}

		// make sure term is out-of-date resp will not be handlered
		if rf.currentTerm > reply.Term {
			return fmt.Errorf("follower term invalid, curT:%d > rpcT:%d", rf.currentTerm, reply.Term)
		}
		return nil
	}

	// update some log status
	updateLogIndex := func() error {
		if reply.Success {

			if rf.matchIndex[server] > args.PrevLogIndex+len(args.Entries) {
				return nil
			}

			if rf.nextIndex[server] != args.PrevLogIndex+1 {
				return nil
			}

			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] += len(args.Entries)
			DebugPretty(dLeader, "S%d -> S%d ok, update matchIndex %d nextIdx:%d (PLI:%d len(entries):%d) at:%v", rf.me, server, rf.matchIndex[server], rf.nextIndex[server], args.PrevLogIndex, len(args.Entries), time.Now().UnixMilli())
		} else {
			rf.nextIndex[server] = reply.NextIndex
			DebugPretty(dLeader, "S%d -> S%d ✘, set nextIndex %d, at:%v", rf.me, server, rf.nextIndex[server], time.Now().UnixMilli())
		}

		return nil

	}

	atomic.AddInt32(count, 1)
	rf.funcWrapperWithStateProtect(termValidate, updateLogIndex)
	DebugPretty(dLog, "S%d -> S%d Done AppEnt  %v", rf.me, server, time.Now().UnixMilli())
}

// StartAppendEntries issue an new AppendLogEntriesRPC witout raft state machined lock
func (rf *Raft) StartAppendEntries() {
	var (
		count       int32
		ctx, cancel = context.WithTimeout(context.Background(), defaultHeartbeatPeriod/2)
	)
	defer cancel()

	concurApEnt := func() error {
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      []LogEntry{}, // for empty appendtry , if log is not empty, then update it
			LeaderCommit: rf.commitIndex,
		}

		logCopy := make([]LogEntry, len(rf.log))
		copy(logCopy, rf.log)
		DebugPretty(dLeader, "S%d Issue AppEnt T:%d MI:%v NI:%v len(log):%v - %v", rf.me, rf.currentTerm, rf.matchIndex, rf.nextIndex, len(rf.log), time.Now().UnixMilli())
		for serverIdx := range rf.peers {
			if serverIdx == rf.me {
				continue
			}
			args.PrevLogIndex = rf.nextIndex[serverIdx] - 1
			if args.PrevLogIndex == 0 {
				args.PrevLogTerm = -1
				args.Entries = logCopy
			} else {
				args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
				args.Entries = logCopy[args.PrevLogIndex:]
			}

			// DebugPretty(dLeader, "S%d -> S%d  Sending PLI:%d PLT:%d N:%d LC:%d at T:%d- entries: %v - %v", rf.me, serverIdx, args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[serverIdx], rf.commitIndex, args.Term, args.Entries, time.Now().UnixMilli())
			go rf.IssueRequestAppendEntries(&count, serverIdx, args)

		}

		rf.updateLeaderCommitIndexAndApplyLogs()

		return nil
	}

	rf.funcWrapperWithStateProtect(concurApEnt)

	for {
		select {
		case <-ctx.Done():
			goto END
		default:
			if atomic.LoadInt32(&count) > int32(len(rf.peers)/2) {
				DebugPretty(dLeader, "S%d Send %d Success %d ", rf.me, atomic.LoadInt32(&count), time.Now().UnixMilli())
				goto END
			}
		}
		time.Sleep(defaultTickerPeriod / 10)
	}
END:
	rf.funcWrapperWithStateProtect(func() error { rf.updateLeaderCommitIndexAndApplyLogs(); return nil })
	DebugPretty(dLeader, "S%d Done Issue AppEnt RPC %d", rf.me, time.Now().UnixMilli())
}

// Raft represent raft
func (rf *Raft) updateLeaderCommitIndexAndApplyLogs() bool {
	if len(rf.log) == 0 {
		return false
	}

	for n := len(rf.log); n > rf.commitIndex; n-- {
		majority := 0
		for sverIdx := range rf.peers {
			if rf.matchIndex[sverIdx] >= n && rf.log[n-1].Term == rf.currentTerm {
				majority++
			}
			if majority > len(rf.peers)/2 {
				rf.commitIndex = n
				DebugPretty(dCommit, "S%d Update Leader CMI to %d - %v", rf.me, rf.commitIndex, time.Now().UnixMilli())
				break
			}
		}
	}
	rf.applyLogEntry()
	return false
}

// Raft represent raft
func (rf *Raft) updateFollowerCommitIndex(args *AppendEntriesArgs) {
	// if leader's commitIndex is larger than follower's commit
	// then set outself commitIndex to min(len(rf.log), leader'CommitIndex)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log) {
			rf.commitIndex = len(rf.log)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		DebugPretty(dCommit, "S%d uppdate commitInex to %d", rf.me, rf.commitIndex)
	}
}

// Raft represent raft
func (rf *Raft) applyLogEntry() {
	if rf.commitIndex > rf.lastApplied {
		n := rf.commitIndex - rf.lastApplied
		for n = rf.lastApplied; n < rf.commitIndex; n++ {
			msg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[rf.lastApplied].Command,
				CommandIndex:  rf.lastApplied + 1,
				SnapshotValid: false,
				Snapshot:      []byte{},
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			rf.applyCh <- msg
			rf.lastApplied++
		}
		DebugPretty(dCommit, "S%d Applied %d logs, lastApplied:%d -- %v ", rf.me, n, rf.lastApplied, time.Now().UnixMilli())
	}
}

// Raft represent raft
func (rf *Raft) findFollowerNextIndex(args *AppendEntriesArgs) bool {
	// case leader 是空,PLI 0, follower 一堆没用数据
	if args.PrevLogIndex == 0 {
		return true
	}

	// follower日志更长,直接比较，比较失败设置nextIndex为
	if len(rf.log) >= args.PrevLogIndex {
		if rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm {
			return true
		}
	}

	// leader日志更长
	if len(rf.log) < args.PrevLogIndex {
		return false
	}

	return false

}
