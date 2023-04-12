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

	termChecker := func() error {

		// if rpcs term is smaller than ourself's term ,then we shoud reconized this request is illegal, then drop it
		if rf.currentTerm > args.Term {
			err := fmt.Errorf("chk term faied, curT:%d > rpcT:%d", rf.currentTerm, reply.Term)
			reply.Term = rf.currentTerm
			rf.persist()
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

	var msgs []ApplyMsg
	logHandler := func() error {
		if rf.commitIndex < rf.lastIncludedIndex {
			reply.NextIndex = rf.lastIncludedIndex + 1
		} else {
			reply.NextIndex = rf.commitIndex + 1
		}

		if rf.commitIndex > args.PrevLogIndex {
			rf.persist()
			return fmt.Errorf("CMI:%d > PLI:%d(for commited log cannot be deleted)", rf.commitIndex, args.PrevLogIndex)
		}

		if ok := rf.findFollowerNextIndex(args); !ok {
			rf.persist()
			return fmt.Errorf("notice Leader update nextIndex=%d", reply.NextIndex)
		}

		fixedIndex := args.PrevLogIndex - rf.lastIncludedIndex

		// findFollerNextIndex make sure that fixedIndex > 0, and fixedIndex in [0, len(rf.logs))
		rf.logs = rf.logs[:fixedIndex]
		rf.logs = append(rf.logs, args.Entries...)

		DebugPretty(dLog, "S%d ->S%d Saved Logs[%d](PLI:%v PLT:%v) [LI:%d CI:%d] at:T%d  all:%v - %v", rf.me, args.LeaderId, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, rf.lastApplied, rf.commitIndex, reply.Term, len(rf.logs), time.Now().UnixMilli())

		rf.updateFollowerCommitIndex(args)
		rf.applyEntryToStateMachine(&msgs)

		reply.Success = true
		return nil
	}

	rf.funcWrapperWithStateProtect(termChecker, logHandler, func() error { rf.persist(); return nil })

	for _, msg := range msgs {
		rf.applyCh <- msg
	}

	DebugPretty(dLog, "S%d Exit receive AppEnt <- S%d (PLI:%d PLT:%d, LCMI:%d, LT:%d) - %d", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Term, time.Now().UnixMilli())
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

		rf.persist() // 2C

		logCopy := make([]LogEntry, len(rf.logs))
		copy(logCopy, rf.logs)
		DebugPretty(dLeader, "S%d Issue AppEnt T:%d MI:%v NI:%v LII:%d, CMI:%d len(log):%v - %v", rf.me, rf.currentTerm, rf.matchIndex, rf.nextIndex, rf.lastIncludedIndex, rf.commitIndex, len(rf.logs), time.Now().UnixMilli())
		for serverIdx := range rf.peers {
			if serverIdx == rf.me {
				continue
			}

			args.PrevLogIndex = rf.nextIndex[serverIdx] - 1

			// If args.PrevLogIndex < rf.lastIncludedIndex it means there some logs that will be send to follower are located at snapshots,
			// In this case we should send leader's snapshot to follower first;
			if args.PrevLogIndex < rf.lastIncludedIndex {
				// DebugPretty(dSnap, "S%d -> S%d send snapshot, prevLogIndex: %d lastIncludeIndex:%d  logs:%v", rf.me, serverIdx, args.PrevLogIndex, rf.lastIncludedIndex, rf.logs)
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

			go rf.IssueRequestAppendEntries(&count, serverIdx, args)
		}

		return nil
	}

	rf.funcWrapperWithStateProtect(concurApEnt)

	// apply to statemachine and send rpc can be parallel for performance

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
		time.Sleep(defaultTickerPeriod)
	}
END:
	var msgs []ApplyMsg
	rf.funcWrapperWithStateProtect(func() error {
		rf.updateLeaderCommitIndex()
		rf.applyEntryToStateMachine(&msgs)
		return nil
	})
	for _, msg := range msgs {
		rf.applyCh <- msg
	}

	DebugPretty(dLeader, "S%d Done Start AppEnt RPC %d", rf.me, time.Now().UnixMilli())
}

// Raft represent raft
func (rf *Raft) updateLeaderCommitIndex() bool {
	if len(rf.logs) == 0 {
		return false
	}

	// DebugPretty(dInfo, "S%d Begin Update CMI: LII:%d CMI:%d len(log)=%d, MatchIndex:%v | log:%v,  CT:%d", rf.me, rf.lastIncludedIndex, rf.commitIndex, len(rf.logs), rf.matchIndex, rf.logs, rf.currentTerm)

	for n := len(rf.logs) + rf.lastIncludedIndex; n > rf.commitIndex && n > rf.lastIncludedIndex; n-- {
		majority := 0
		for sverIdx := range rf.peers {
			if rf.matchIndex[sverIdx] >= n && rf.logs[n-rf.lastIncludedIndex-1].Term == rf.currentTerm {
				majority++
			}
			if majority > len(rf.peers)/2 {
				DebugPretty(dCommit, "S%d Update CMI %d -> %d ", rf.me, rf.commitIndex, n)
				rf.commitIndex = n
				break
			}
		}
	}
	return false
}

// Raft represent raft
func (rf *Raft) updateFollowerCommitIndex(args *AppendEntriesArgs) {
	// if leader's commitIndex is larger than follower's commit
	// then set outself commitIndex to min(len(rf.log), leader'CommitIndex)
	oldCmi := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logs)+rf.lastIncludedIndex {
			rf.commitIndex = len(rf.logs) + rf.lastIncludedIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		DebugPretty(dCommit, "S%d UpdateCMI (LCMI:%d CMI:%d LenLog:%d) -> %d", rf.me, args.LeaderCommit, oldCmi, len(rf.logs), rf.commitIndex)
	}
}

// applyEntryToStateMachine apply log entry to state machine
func (rf *Raft) applyEntryToStateMachine(entries *[]ApplyMsg) {
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}

	if rf.commitIndex < rf.lastApplied {
		*entries = append(*entries, ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  0,
			SnapshotValid: true,
			Snapshot:      rf.persister.ReadSnapshot(),
			SnapshotTerm:  rf.lastIncludedTerm,
			SnapshotIndex: rf.lastIncludedIndex,
		})
		DebugPretty(dCommit, "S%d Applied Snapshot, lastApplied: %d, CurLogSize:%d", rf.me, rf.lastApplied, len(rf.logs))
		return
	}

	DebugPretty(dInfo, "S%d Ready Apply CMI:%d LII:%d LAI:%d LenLog:%d", rf.me, rf.commitIndex, rf.lastIncludedIndex, rf.lastApplied, len(rf.logs))
	if rf.commitIndex > rf.lastApplied {
		var count int = 0
		// n := rf.commitIndex - rf.lastApplied
		for n := rf.lastApplied; n < rf.commitIndex; n++ {
			msg := ApplyMsg{
				CommandValid: true,
				// Command:       rf.logs[rf.lastApplied-rf.lastIncludedIndex].Command,
				Command:       rf.logs[n-rf.lastIncludedIndex].Command,
				CommandIndex:  rf.lastApplied + 1,
				SnapshotValid: false,
				Snapshot:      []byte{},
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			*entries = append(*entries, msg)
			rf.lastApplied++
			count++
		}
		rf.persist()
		DebugPretty(dCommit, "S%d Applied %d logs, lastApplied:%d -curLog:%d- %v ", rf.me, count, rf.lastApplied, len(rf.logs), time.Now().UnixMilli())
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
