package raft

import (
	"context"
	"math"
	"sync"
)

type Entry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term      int  // currentTerm, for leader to update itself
	Success   bool // true if follower contained entry matching prevLogIndex and  prevLogTerm
	NextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.NextIndex = rf.commitIndex + 1
	// rule for all server
	rf.applyLogToStateMachine()
	if args.Term < rf.currentTerm {
		DebugPrint(dWarn, "S%d <- S%d, ApEntRPC(Reject)/T%d, Msg(RF_T:%d  LT:%d)", rf.me, args.LeaderId, rf.currentTerm, rf.currentTerm, args.Term)
		return
	}

	DebugPrint(dTimer, "S%d <- S%d, ApEntRPC(RST ELT)/T%d", rf.me, args.LeaderId, rf.currentTerm)
	rf.resetTimer()

	if args.Term > rf.currentTerm {
		DebugPrint(dWarn, "S%d <- S%d, ApEntRPC(Cvt to Follower)T%d, Msg(T%d -> T%d)", rf.me, args.LeaderId, rf.currentTerm, rf.currentTerm, args.Term)
		rf.convertToFollower(args.Term)
	}

	// 2# if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm, return false
	if args.PrevLogIndex != 0 && (len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		if len(rf.log) < args.PrevLogIndex {
			DebugPrint(dWarn, "S%d <- S%d, ApEntRPC(Reject)/T%d, Msg(LL: %d ,PLI: %d)", rf.me, args.LeaderId, rf.currentTerm, len(rf.log), args.PrevLogIndex)
		} else {
			DebugPrint(dWarn, "S%d <- S%d, ApEntRPC(Reject)/T%d, Msg(RF_LLT: %d, PLT: %d)", rf.me, args.LeaderId, rf.currentTerm, rf.log[args.PrevLogIndex-1].Term, args.PrevLogTerm)
		}
		return
	}

	// if existed an entry that conflicts with new one (same index, but not same term), delete the entry and all that follow it
	if len(rf.log) != 0 {
		for i, j := args.PrevLogIndex, 0; i < len(rf.log) && j < len(args.Entries); i, j = i+1, j+1 {
			if rf.log[i].Term != args.Entries[j].Term {
				rf.log = rf.log[:i]
				break
			}
		}
	}

	// figure4, Checkpoint#4
	// Append new entry that not in the log
	rf.log = append(rf.log, args.Entries...)
	// figure5, Checkpoint#5
	if rf.commitIndex < args.LeaderCommit {
		oldCI := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log))))
		DebugPrint(dLog2, "S%d <- S%d, ApEntRPC(CMI UPT)/T%d, Msg(T%d -> T%d)", rf.me, args.LeaderId, rf.currentTerm, oldCI, rf.commitIndex)
	}
	reply.Success = true
	DebugPrint(dLog2, "S%d <- S%d, ApEntRPC(Log Saved)/T%d, CLL: %d Msg(PLI: %d PLT: %d EntryL: %v)", rf.me, args.LeaderId, rf.currentTerm, len(rf.log), args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, reply)
	return ok
}

func (rf *Raft) doAppendEntries(ctx context.Context) {
	var (
		wg *sync.WaitGroup = &sync.WaitGroup{}
	)
	defer func() {
		wg.Wait()
		rf.wg.Done()
	}()
	select {
	case <-rf.ctx.Done():
		return
	default:
	}
	rf.mu.Lock()
	if rf.serverState != Leader {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      []Entry{},
		LeaderCommit: rf.commitIndex,
	}

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		args.PrevLogIndex = rf.nextIndex[server] - 1
		if args.PrevLogIndex == 0 {
			args.PrevLogTerm = -1
		} else {
			args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
		}
		//args.Entries = rf.log[args.PrevLogIndex:]
		args.Entries = make([]Entry, len(rf.log)-args.PrevLogIndex)
		copy(args.Entries, rf.log[args.PrevLogIndex:])
		wg.Add(1)
		// PLI prevLogIndex PLT: prevLogTerm N:
		DebugPrint(dLog, "S%d -> S%d, ApEntRPC(Sending)/T%d,  Msg(PLI: %d  PLT: %d ENL: %d LC: %d) - EntryLen: %d",
			rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, len(args.Entries))
		go rf.wrapSendAppendEntries(wg, server, args)
	}
	rf.mu.Unlock()
}

func (rf *Raft) wrapSendAppendEntries(wg *sync.WaitGroup, server int, args AppendEntriesArgs) {
	defer func() {
		wg.Done()
	}()

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		return
	}

	if rf.serverState != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		DebugPrint(dWarn, "S%d -> S%d, ApEntRPC(Cvt Follower)/T%d, Msg(T%d->T%d)", rf.me, server, currentTerm, currentTerm, reply.Term)
		rf.convertToFollower(reply.Term)
	} else {
		// todo for lab2b,2c,2d
		if !reply.Success && rf.nextIndex[server] > 1 {
			prevNextIndex := rf.nextIndex[server]
			rf.nextIndex[server] = reply.NextIndex
			DebugPrint(dWarn, "S%d -> S%d, ApEntRPC(NotMatch)/T%d, NI(%d->%d), Repl(S:%v,T:%v, NI: %d)", rf.me, server, currentTerm, prevNextIndex, rf.nextIndex[server], reply.Success, reply.Term, reply.NextIndex)
		} else {
			rf.nextIndex[server] += len(args.Entries)
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		}
	}
}
