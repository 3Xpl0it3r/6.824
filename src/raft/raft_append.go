package raft

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int 		// currentTerm, for leader to update itself
	Success bool		// true if follower contained entry matching prevLogIndex and  prevLogTerm
}


func(rf *Raft)AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		return
	}
	if args.Term > rf.currentTerm{
		rf.convertToFollower(args.Term)
	}
	rf.resetTimer()
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply)  bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

