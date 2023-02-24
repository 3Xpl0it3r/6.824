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
	DebugPretty(dSnap, "S%d <- S%d Rcv Snapshot", rf.me, args.LeaderId)

	termvalidater := func() error {
		if args.Term < rf.currentTerm {
			rf.persist()
			reply.Term = rf.currentTerm
			err := fmt.Errorf("chk term failed, curT:%d rpcT:%d", rf.currentTerm, args.Term)
			return err
		}
		// if ourself term is smaller than rpc's term, then we should immediately translate to follower whatever role we now in
		rf.switchState(Any, Follower, func() {
			if rf.currentTerm < args.Term {
				rf.updateTerm(args.Term)
			}
		})
		rf.resetElectionTimer()

		reply.Term = rf.currentTerm
		return nil
	}

	snapshotHandler := func() error {
		rf.persistSnapshot(args.LastIncludedIndex, args.Data)
		if !args.Done {
			rf.persist()
			return nil
		}
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		if len(rf.logs)+rf.lastIncludedIndex > args.LastIncludedIndex {
			if rf.logs[args.LastIncludedIndex-1].Term == args.LastIncludedTerm {
				rf.logs = rf.logs[args.LastIncludedIndex:]
			}
		}
		if rf.commitIndex < args.LastIncludedIndex {
			rf.commitIndex = args.LastIncludedIndex
		}
		if rf.lastApplied < args.LastIncludedIndex {
			rf.lastApplied = args.LastIncludedIndex
		}
		return nil
	}

	rf.funcWrapperWithStateProtect(termvalidater, snapshotHandler, func() error { rf.persist(); return nil })
}

// sendInstallSnapshotRPC [#TODO](should add some comments)
func (rf *Raft) sendInstallSnapshotRPC(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshotRPC", args, reply)
	return ok
}

// IssueInstallSnapshotRPC [#TODO](should add some comments)
func (rf *Raft) IssueInstallSnapshotRPC(server int, args SnapshotArgs) {
	DebugPretty(dSnap, "S%d -> S%d Snapshot", rf.me, server)

	reply := SnapshotReply{}
	if rf.sendInstallSnapshotRPC(server, &args, &reply) {
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
			return err
		}
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

	rf.persistSnapshot(index, snapshot)
	// rf.funcWrapperWithStateProtect(func() error { rf.persistSnapshot(index, snapshot); return nil })


}

// persistSnapshot [#TODO](should add some comments)
func (rf *Raft) persistSnapshot(index int, snapshot []byte) {
	if rf.role == Leader {
		DebugPretty(dSnap, "S%d Begin Snapshot at %d(LLI:%d LLT:%d), rf.logs:%v", index, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.logs)
	}
	if rf.lastIncludedIndex == index {
		return
	}
	rf.lastIncludedTerm = rf.logs[index-rf.lastIncludedIndex-1].Term
	rf.lastIncludedIndex = index
	rf.logs = rf.logs[index-rf.lastIncludedIndex:]

	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.voteFor)
	encoder.Encode(rf.logs)
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.lastIncludedTerm)

	rf.persister.SaveStateAndSnapshot(writer.Bytes(), snapshot)

	if rf.role == Leader {
		DebugPretty(dSnap, "S%d Complete Snapshot at %d(LLI:%d LLT:%d), rf.logs:%v", index, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.logs)
	}
}
