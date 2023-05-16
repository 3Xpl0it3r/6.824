package raft

// runConfig represent runconfig
type runtimeState struct {
	commitIndex int
	lastApplied int

	lastIncludeTerm  int
	lastIncludeIndex int
	term             int
	logs             []LogEntry
}

// takeRunConfig [#TODO](should add some comments)
func (rf *Raft) runtimeStateSnapshot() runtimeState {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rt := runtimeState{
		commitIndex:      rf.commitIndex,
		lastApplied:      rf.lastApplied,
		lastIncludeTerm:  rf.lastIncludedTerm,
		lastIncludeIndex: rf.lastIncludedIndex,
		term:             rf.currentTerm,
	}
	rt.logs = make([]LogEntry, len(rf.logs))
	copy(rt.logs, rf.logs)
	return rt
}

// runtimeStateCAS [#TODO](should add some comments)
func (rf *Raft) runtimeStateCAS(rtSnap runtimeState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < rtSnap.commitIndex {
		rf.commitIndex = rtSnap.commitIndex
	}
	if rf.lastIncludedTerm <= rtSnap.lastIncludeTerm {
		rf.lastIncludedTerm = rtSnap.lastIncludeTerm
	}

	rf.lastApplied = rtSnap.lastApplied

	rf.lastIncludedIndex = rtSnap.lastIncludeIndex

}
