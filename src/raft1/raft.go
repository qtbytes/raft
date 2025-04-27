package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const BROADCAST_TIME = time.Millisecond * 100 // Heartbeat wait time
const ELECTION_TIMEOUT_MIN = 10 * BROADCAST_TIME
const ELECTION_TIMEOUT_MAX = 20 * BROADCAST_TIME

const (
	LEADER    = "Leader"
	FOLLOWER  = "Follower"
	CANDIDATE = "Candidate"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state           string    // leader, follower, candidate
	lastTime        time.Time // the last time received heartbeat
	electionTimeout time.Duration
	voteCount       int // vote count when election

	applyCh   chan raftapi.ApplyMsg // store committed entry
	needApply sync.Cond

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// --------------------- Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)

	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int
	// candidateId that received vote in current term (or null if none)
	votedFor int
	len      int
	// log entries; each entry contains command for state machine, and term when entry
	// was received by leader (first index is 1)
	log []LogEntry

	// --------------------- Volatile state on all servers:

	// index of highest log entry known to be committed
	// (initialized to 0, increases monotonically)
	commitIndex int
	// index of highest log entry applied to state machine
	// (initialized to 0, increases monotonically)
	lastApplied int

	// --------------------- Volatile state on leaders: (Reinitialized after election)
	// for each server, index of the next log entry
	// to send to that server (initialized to leader last log index + 1)
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically)
	matchIndex []int

	snapShot      []byte
	snapShotIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.state == LEADER

	return term, isleader
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// we don't delete log, use a offset replace

	// for i, entry := range rf.log {
	// 	if entry.Index > index {
	// 		DPrintf("Got snapshot start at %v, remove logs whose index < %v", index, index)
	// 		DPrintf("Before: %v", rf.log)
	// 		rf.log = rf.log[i:]
	// 		DPrintf("After: %v", rf.log)
	// 		break
	// 	}
	// }

	rf.snapShot = snapshot
	rf.snapShotIndex = max(rf.snapShotIndex, index)

}

// goroutine for leader to send heartbeat to followers
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (int, int, bool) {
	// Your code here (3B).
	if !rf.isLeader() {
		return 0, 0, false
	}

	rf.mu.Lock()

	index := rf.len
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	entry := LogEntry{term, index, raftapi.ApplyMsg{CommandValid: true, Command: command, CommandIndex: index}}

	DPrintf("%v %v receive log entry %v from clients", rf.state, rf.me, entry)

	rf.log = append(rf.log, entry)
	rf.len++
	rf.persist()

	rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me {
			go func(server int) {
				rf.sendAppendEntries(server, false)
			}(server)
		}
	}

	go rf.updateCommitIndex()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister,
	applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.mu = sync.Mutex{}
	rf.needApply = *sync.NewCond(&rf.mu)

	// Your initialization code here (3A, 3B, 3C).
	rf.state = FOLLOWER
	rf.resetElectionTimer()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.initLog()
	rf.len = 1

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.apply()

	return rf
}
