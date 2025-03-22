package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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

	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return -1, -1, false
	}
	entry := LogEntry{
		Term:  rf.currentTerm,
		Index: len(rf.log),
		Entry: raftapi.ApplyMsg{CommandValid: true, Command: command, CommandIndex: len(rf.log)},
	}

	DPrintf("%v %v receive log entry %+v from clients", rf.state, rf.me, entry)
	rf.log = append(rf.log, entry)

	rf.mu.Unlock()
	// DPrintf("%v %v commitIndex: %v %v", rf.state, rf.me, rf.commitIndex, rf.log)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		rf.mu.Lock()
		currentTerm := rf.currentTerm
		nextIndex := rf.nextIndex[server]
		// If last log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with log entries starting at nextIndex
		entries := rf.log[nextIndex:]
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		leaderCommit := rf.commitIndex
		rf.mu.Unlock()

		go func(server, currentTerm, prevLogIndex, prevLogTerm int, entries []LogEntry, leaderCommit int) {
			DPrintf("%v %v send %v to follower %v", rf.state, rf.me, entries, server)
			args := RequestAppendArgs{
				Term:         currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			reply := RequestAppendReply{}
			rf.sendAppendEntries(server, &args, &reply)
		}(server, currentTerm, prevLogIndex, prevLogTerm, entries, leaderCommit)
	}
	go rf.checkMatchIndex()

	// Need to wait for follower commit
	// time.Sleep(2 * BROADCAST_TIME)
	// rf.needApply.L.Lock()
	// for !(rf.commitIndex == len(rf.log)-1) {
	// 	rf.needApply.Wait()
	// }
	// DPrintf("%v %v %v", rf.commitIndex, rf.currentTerm, rf.state == LEADER)
	// rf.needApply.L.Unlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log) - 1, rf.currentTerm, (rf.state == LEADER)
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
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
	rf.log = []LogEntry{{Term: 0, Index: 0}} // sentinel

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.apply()

	return rf
}
