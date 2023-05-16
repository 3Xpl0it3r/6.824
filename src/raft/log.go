package raft

import "time"

// applyLogEntries [#TODO](should add some comments)
func (rf *Raft) applyLogEntries() {
	var needPersisent bool = false
	rf.mu.Lock()
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	if rf.commitIndex < rf.lastApplied {
		needPersisent = true
		lastIncludedIndex, lastIncludeTerm := rf.lastIncludedIndex, rf.lastIncludedTerm
		DebugPretty(dCommit, "S%d Applied Snapshot, lastApplied: %d, CurLogSize:%d", rf.me, rf.lastApplied, len(rf.logs))
		rf.mu.Unlock()
		rf.applySnapshot(lastIncludeTerm, lastIncludedIndex)
		goto END
	}
	rf.mu.Unlock()

	needPersisent = rf.applyCommonCommand()

END:
	if needPersisent {
		rf.mu.Lock()
		rf.persist()
		rf.mu.Unlock()
	}
}

// applySnapshot [#TODO](should add some comments)
func (rf *Raft) applySnapshot(snapshotTerm, snapshotindex int) {
	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
}

// applyCommonCommand apply log entry to state machine
func (rf *Raft) applyCommonCommand() bool {
	var n int
	for true {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			break
		}
		msg := ApplyMsg{
			CommandValid: true,
			// Command:       rf.logs[rf.lastApplied-rf.lastIncludedIndex].Command,
			Command:       rf.logs[rf.lastApplied-rf.lastIncludedIndex].Command,
			CommandIndex:  rf.lastApplied + 1,
			SnapshotValid: false,
			Snapshot:      []byte{},
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		rf.lastApplied++
		rf.mu.Unlock()
		rf.applyCh <- msg
		n++
	}
	if n > 0 {
		DebugPretty(dCommit, "S%d commit %d logs - %v ", rf.me, n, time.Now().UnixMilli())
		return true

	}
	return false
	// }
}
