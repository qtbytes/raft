package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
func (rf *Raft) resetElectionTimer() {
	timeout := ELECTION_TIMEOUT_MIN + time.Duration(rand.Int63()%
		int64(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN))
	rf.electionTimeout = timeout
	rf.lastTime = time.Now()
}
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		// DPrintf("%v %v term: %v", rf.state, rf.me, rf.currentTerm)

		if rf.state != LEADER && time.Since(rf.lastTime) >= rf.electionTimeout {
			DPrintf("%v %v: don't receive heartbeat after %v, start election\n", rf.state, rf.me, rf.electionTimeout)
			rf.mu.Unlock()
			rf.startElection()
			return
		} else {
			rf.mu.Unlock()
		}

		// The paper's Section 5.2 mentions election timeouts in the range of
		// 150 to 300 milliseconds. Such a range only makes sense
		// if the leader sends heartbeats considerably more often than
		// once per 150 milliseconds (e.g., once per 10 milliseconds).
		// Because the tester limits you tens of heartbeats per second,
		// you will have to use an election timeout larger than the paper's
		// 150 to 300 milliseconds, but not too large,
		// because then you may fail to elect a leader within five seconds.

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) initIndex() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}
}
