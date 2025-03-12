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

const ELECTION_TIMEOUT = time.Second

// var ELECTION_TIMEOUT = time.Duration(300+(rand.Int63()%300)) * time.Millisecond

const (
	LEADER    = "Leader"
	FOLLOWER  = "Follower"
	CANDIDATE = "Candidate"
)

type LogEntry struct {
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state    string    // leader, follower, candidate
	lastTime time.Time // the last time received heartbeat
	count    int       // vote count when election
	LeaderID int

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// ---------------------
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)

	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int
	// candidateId that received vote in current term (or null if none)
	votedFor int
	// log entries; each entry contains command for state machine, and term when entry
	// was received by leader (first index is 1)
	log []LogEntry

	// ---------------------
	// Volatile state on leaders: (Reinitialized after election)

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

type RequestAppendArgs struct {
	Term     int // leader’s term
	LeaderID int // so follower can redirect clients

}
type RequestAppendReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

}

func (rf *Raft) AppendEntries(args *RequestAppendArgs, reply *RequestAppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastTime = time.Now()
	DPrintf("%v %v has term: %v, received heartbeat from leader %v with term %v", rf.state, rf.me, rf.currentTerm, args.LeaderID, args.Term)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.LeaderID = args.LeaderID
		rf.state = FOLLOWER
		reply.Success = true
	} else if args.Term == rf.currentTerm {
		if rf.state == CANDIDATE {
			rf.state = FOLLOWER
		}
		rf.LeaderID = args.LeaderID
		reply.Success = true
	} else {
		// TODO
		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
		reply.Success = false
	}
	reply.Term = rf.currentTerm
}
func (rf *Raft) sendAppendEntries(server int, args *RequestAppendArgs, reply *RequestAppendReply) {
	for {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			return
		}
		// TODO: Maybe need a better time
		time.Sleep(100 * time.Millisecond)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Update heartbeat
	rf.lastTime = time.Now()

	if args.Term < rf.currentTerm {
		DPrintf("%v %v don't vote for Candidate %v", rf.state, rf.me, args.CandidateID)
		DPrintf("%v %v term %v > Candidate %v term %v", rf.state, rf.me, rf.currentTerm, args.CandidateID, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		DPrintf("%v %v don't vote for Candidate %v", rf.state, rf.me, args.CandidateID)
		DPrintf("%v %v term %v < Candidate %v term %v", rf.state, rf.me, rf.currentTerm, args.CandidateID, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		DPrintf("%v %v vote for Candidate %v", rf.state, rf.me, args.CandidateID)
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	} else {
		DPrintf("%v %v don't vote for Candidate %v", rf.state, rf.me, args.CandidateID)
		DPrintf("%v %v already voted for Candidate %v", rf.state, rf.me, rf.votedFor)
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	if reply.VoteGranted {
		rf.count++
		DPrintf("%v %v got vote from %v", rf.state, rf.me, server)
		// (a) it wins the election
		if rf.count > len(rf.peers)/2 && rf.state == CANDIDATE {
			DPrintf("%v %v got %v votes, win election with term %v", rf.state, rf.me, rf.count, rf.currentTerm)
			rf.state = LEADER
			go rf.sendHeartBeat()
			return
		}
	}
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state != FOLLOWER {
		rf.mu.Unlock()
		return
	}

	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastTime = time.Now() // reset timer
	rf.count = 1

	state, me, term := rf.state, rf.me, rf.currentTerm
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != me {
			go func(server int) {
				DPrintf("%v %v send request vote to %v", state, me, server)
				args := RequestVoteArgs{
					Term:        term,
					CandidateID: me,
				}
				reply := RequestVoteReply{}
				rf.sendRequestVote(server, &args, &reply)
			}(server)
		}
	}
	// TODO: wait for goroutine
	// time.Sleep(10 * time.Millisecond)

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// (b) another server establishes itself as leader
	// if rf.state == FOLLOWER {
	// 	return
	// }
	// (c) a period of time goes by with no winner
	// randomized election timeouts
	// time.Sleep(time.Second * (150 + time.Duration(rand.Int63()%150)))
}
func (rf *Raft) sendHeartBeat() {
	for server := range rf.peers {
		if server != rf.me {
			args := RequestAppendArgs{LeaderID: rf.me, Term: rf.currentTerm}
			reply := RequestAppendReply{}
			DPrintf("%v %v sending heartbeat to %v", rf.state, rf.me, server)
			// Maybe need catch return value from rpc
			go rf.sendAppendEntries(server, &args, &reply)
		}
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		// DPrintf("%v %v term: %v", rf.state, rf.me, rf.currentTerm)

		// if rf.state == LEADER {
		// 	rf.mu.Unlock()
		// 	rf.sendHeartBeat()
		// 	return
		// }

		if rf.state != LEADER && time.Since(rf.lastTime) >= ELECTION_TIMEOUT {
			DPrintf("%v %v: Timeout, start election\n", rf.state, rf.me)
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

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
func (rf *Raft) heartbeat() {
	for !rf.killed() {

		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.sendHeartBeat()
			return
		}

		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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

	// Your initialization code here (3A, 3B, 3C).
	rf.state = FOLLOWER
	rf.lastTime = time.Now()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// go rf.heartbeat()

	return rf
}
