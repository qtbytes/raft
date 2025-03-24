package raft

import (
	"log"
	"math/rand"
	"sort"
	"time"

	"6.5840/raftapi"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
func (rf *Raft) resetElectionTimer() {
	rf.lastTime = time.Now()
	// DPrintf("%v %v reset election timer to %v", rf.state, rf.me, rf.lastTime)
	timeout := ELECTION_TIMEOUT_MIN + time.Duration(rand.Int63()%
		int64(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN))
	rf.electionTimeout = timeout
}
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		isLeader := rf.state == LEADER
		timeElapsed := time.Since(rf.lastTime)
		electionTimeout := rf.electionTimeout
		rf.mu.Unlock()

		// DPrintf("%v %v term: %v", rf.state, rf.me, rf.currentTerm)

		if !isLeader && timeElapsed >= electionTimeout {
			DPrintf("%v %v: don't receive heartbeat after %v, start election\n",
				rf.state, rf.me, rf.electionTimeout)
			rf.startElection()
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
		rf.matchIndex[i] = 0
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4)
func (rf *Raft) updateCommitIndex() {
	for !rf.killed() {

		rf.mu.Lock()

		sorted := make([]int, len(rf.matchIndex))
		copy(sorted, rf.matchIndex)
		sort.Ints(sorted)
		n := sorted[(len(rf.peers)+1)/2]
		if n > rf.commitIndex && rf.log[n].Term == rf.currentTerm {
			DPrintf("After check matchIndex, %v %v update commitIndex to %v", rf.state, rf.me, n)
			rf.commitIndex = n
			rf.needApply.Broadcast()
		}

		rf.mu.Unlock()

		time.Sleep(BROADCAST_TIME)
	}
}

// Rules for Servers:
// If commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine (§5.3)
func (rf *Raft) apply() {
	for !rf.killed() {
		rf.needApply.L.Lock()
		for !(rf.commitIndex > rf.lastApplied) {
			rf.needApply.Wait()
		}

		rf.lastApplied++
		DPrintf("%v %v update lastApplied to %v", rf.state, rf.me, rf.lastApplied)
		DPrintf("%v %v apply log[%v] to state machine", rf.state, rf.me, rf.lastApplied)

		go func(entry raftapi.ApplyMsg) {
			if entry.CommandValid {
				rf.applyCh <- entry
			}
		}(rf.log[rf.lastApplied].Entry)

		rf.needApply.L.Unlock()
	}
}
