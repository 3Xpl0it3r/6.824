package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type RaftProtectLevel int32

const (
	LevelLogSS RaftProtectLevel = iota
	LevelRaftSM
	LevelAllProtect
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DebugPretty(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity > 0 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// Raft represent raft
func (rf *Raft) funcWrapperWithStateProtect(fns ...func() error) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for _, fn := range fns {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

//go:inline
func randomizedElectionTimeout() time.Duration {
	return time.Duration((rand.Intn(int(defaultMinEelectionTimeout)) + int(defaultMinEelectionTimeout)))
}

// Raft represent raft
func (rf *Raft) switchState(from, to Role, stateFn func()) {

	if from != Any && rf.role != from {
		err := fmt.Errorf("switch %s -> %s failed, expect: %d ,but got %d", from, to, from, rf.role)
		panic(err)
	}

	// this is default config
	rf.role = to

	// hook should be overwrite default if necessary
	if stateFn != nil {
		stateFn()
	}
}

// Raft represent raft
func (rf *Raft) resetElectionTimer() {
	rf.termAt = time.Now()
}

// Raft represent raft
func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term
}

// Raft represent raft
func (rf *Raft) updateVoteFor(vote int) {
	rf.voteFor = vote
}
