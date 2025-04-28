package raft

import (
	"log"
	"math/rand"
	"os"
	"sort"
	"time"
)

// Debugging

func DPrintf(format string, a ...interface{}) {
	if os.Getenv("DEBUG") == "1" {
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
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.len()
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
		N := sorted[(len(rf.peers)+1)/2]
		if N > rf.commitIndex && N < rf.len() && rf.get(N).Term == rf.currentTerm {
			DPrintf("After check matchIndex, %v %v update commitIndex to %v", rf.state, rf.me, N)
			rf.commitIndex = N
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
		start := rf.lastApplied
		end := rf.commitIndex
		entries := make([]LogEntry, end-start)
		copy(entries, rf.log[start+1-rf.snapShotIndex():end+1-rf.snapShotIndex()])
		rf.needApply.L.Unlock()

		for _, entry := range entries {
			rf.applyCh <- entry.Entry
		}

		rf.mu.Lock()
		DPrintf("%v %v apply entries[%v:%v] in term %v", rf.state, rf.me,
			start+1, end+1, rf.currentTerm)
		rf.lastApplied = max(rf.lastApplied, end)
		DPrintf("%v %v update lastApplied to %v", rf.state, rf.me, rf.lastApplied)
		rf.mu.Unlock()
	}
}

func (rf *Raft) switchToFollower(other int, term int) {
	rf.mu.Lock()
	DPrintf("%v %v find server %v has larger Trem, switch to Follower", rf.state, rf.me, other)
	rf.state = FOLLOWER
	rf.resetElectionTimer()
	rf.votedFor = -1
	rf.currentTerm = term
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == LEADER
}

func (rf *Raft) len() int {
	return rf.log[0].Index + len(rf.log)
}

func (rf *Raft) get(i int) LogEntry {
	return rf.log[i-rf.snapShotIndex()]
}

func (rf *Raft) snapShotTerm() int {
	return rf.log[0].Term
}

func (rf *Raft) snapShotIndex() int {
	return rf.log[0].Index
}

func (rf *Raft) initLog(snapShotTerm int, snapShotIndex int) {
	// Init log with snapshot term and index
	rf.log = []LogEntry{{Term: snapShotTerm, Index: snapShotIndex}}
}

func (rf *Raft) getTerm(i int) (term int) {
	if i-rf.snapShotIndex() == 0 {
		term = rf.snapShotTerm()
	} else {
		term = rf.get(i).Term
	}
	return
}

func (rf *Raft) savePartLog(entries []LogEntry) {
	log := make([]LogEntry, len(entries))
	copy(log[1:], entries[1:])
	log[0] = LogEntry{Term: entries[0].Term, Index: entries[0].Index}
	rf.log = log
}
