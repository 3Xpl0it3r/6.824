package raft

import (
	"bytes"
	"fmt"

	"6.824/labgob"
)

// SnapshotArgs represent snapshotargs
type SnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

// SnapshotReply represent snapshotreply
type SnapshotReply struct {
	Term int // currentTerm , is used for leader to update itseld term
}

// InstallSnapshotRPC [#TODO](should add some comments)
func (rf *Raft) InstallSnapshotRPC(args *SnapshotArgs, reply *SnapshotReply) {

	termChecker := func() error {
		DebugPretty(dSnap, "S%d <- S%d Install Snap (rpc.LLI:%d rpc.LLT:%d)(rf.LLI:%d rf.LLT:%d LEN:%d)", rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.logs))

		if args.Term < rf.currentTerm {
			rf.persist()
			reply.Term = rf.currentTerm
			err := fmt.Errorf("chk term failed, curT:%d rpcT:%d", rf.currentTerm, args.Term)
			DebugPretty(dSnap, "S%d <- S%d Reject snap, %v", rf.me, args.LeaderId, err)
			return err
		}
		// if ourself term is smaller than rpc's term, then we should immediately translate to follower whatever role we now in
		rf.switchState(Any, Follower, func() {
			if rf.currentTerm < args.Term {
				DebugPretty(dTerm, "S%d Recv Snap Update Term T%d -> T%d", rf.me, rf.currentTerm, args.Term)
				rf.updateTerm(args.Term)
			}
		})
		rf.resetElectionTimer()

		reply.Term = rf.currentTerm
		return nil
	}

	installSps := func() error {
		if rf.commitIndex >= args.LastIncludedIndex {
			return nil
		}
		if args.LastIncludedIndex < rf.lastIncludedIndex {
			panic("impossible")
		}

		fixedFindex := args.LastIncludedIndex - rf.lastIncludedIndex

		if fixedFindex <= len(rf.logs) {
			rf.logs = rf.logs[fixedFindex:]
		}
		if fixedFindex > len(rf.logs) {
			rf.logs = rf.logs[:0]
		}

		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm

		rf.persistSnapshot(args.Data)

		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  0,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  rf.lastIncludedTerm,
			SnapshotIndex: rf.lastIncludedIndex,
		}

		// we should install snapshot to statemachine throuhg send an applyMsg that the msg type is snapshot
		// rf.applyEntryToStateMachine(&msgs)

		DebugPretty(dSnap, "S%d Installed Snapsot: rf.LII:%d rf.LIT:%d rf.CMI:%d len(log)=%d", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.commitIndex, len(rf.logs))
		return nil
	}

	rf.funcWrapperWithStateProtect(termChecker, installSps, func() error { rf.persist(); return nil })

}

// sendInstallSnapshotRPC [#TODO](should add some comments)
func (rf *Raft) sendInstallSnapshotRPC(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshotRPC", args, reply)
	return ok
}

// IssueInstallSnapshotRPC [#TODO](should add some comments)
func (rf *Raft) IssueInstallSnapshotRPC(server int, args SnapshotArgs) {
	DebugPretty(dSnap, "S%d -> S%d Snapshot LLI:%d LLT:%d", rf.me, server, args.LastIncludedIndex, args.LastIncludedTerm)

	reply := SnapshotReply{}
	if !rf.sendInstallSnapshotRPC(server, &args, &reply) {
		return
	}

	termValidate := func() error {
		if rf.currentTerm > args.Term {
			return nil
		}

		if rf.currentTerm < reply.Term {
			err := fmt.Errorf("ourself term invalid , ,cvt follower curT:%d -> rpcT:%d", rf.currentTerm, reply.Term)
			rf.switchState(Any, Follower, func() {
				rf.updateTerm(reply.Term)
			})
			DebugPretty(dSnap, "S%d -> S%d Snapfailed %v", rf.me, server, err)
			return err
		}
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		DebugPretty(dSnap, "S%d -> S%d Send Snapshot Done NX:%v", rf.me, server, args.LastIncludedIndex)
		return nil
	}

	rf.funcWrapperWithStateProtect(termValidate)
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
	rf.funcWrapperWithStateProtect(func() error { rf.takeSnapshot(index, snapshot); return nil })
}

// takeSnapshot [#TODO](should add some comments)
func (rf *Raft) takeSnapshot(index int, snapshot []byte) {

	if rf.lastIncludedIndex == index {
		return
	}
	logLen := len(rf.logs)

	// truncated logs
	rf.lastIncludedTerm = rf.logs[index-rf.lastIncludedIndex-1].Term
	rf.logs = rf.logs[index-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = index

	// install snapshot and raft state into persisent
	rf.persistSnapshot(snapshot)

	// debug logs
	DebugPretty(dSnap, "S%d Done Snapshot at %d(LLI:%d LLT:%d), rf.logs:%d -> %d", rf.me, index, rf.lastIncludedIndex, rf.lastIncludedTerm, logLen, len(rf.logs))
}

// only ally snapshot to local state machine
func (rf *Raft) persistSnapshot(snapshot []byte) {

	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.voteFor)
	encoder.Encode(rf.logs)
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.lastIncludedTerm)

	rf.persister.SaveStateAndSnapshot(writer.Bytes(), snapshot)
}
