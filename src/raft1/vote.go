package raft

import "time"

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

	if args.Term < rf.currentTerm {
		DPrintf("%v %v don't vote for Candidate %v, Term: %v > %v", rf.state, rf.me, args.CandidateID, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.resetElectionTimer()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.initIndex() // Init nextIndex and matchIndex
	}

	// check candidate’s log is at least as up-to-date as receiver’s log
	term := rf.log[len(rf.log)-1].Term
	upToDate := args.LastLogTerm > term ||
		(args.LastLogTerm == term && args.LastLogIndex+1 >= len(rf.log))

	if upToDate && (rf.votedFor == -1 || rf.votedFor == args.CandidateID) {
		DPrintf("%v %v vote for Candidate %v", rf.state, rf.me, args.CandidateID)
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	} else {
		if !upToDate {
			DPrintf("%v %v don't vote for Candidate %v, log is more up-to-date", rf.state, rf.me, args.CandidateID)
		} else {
			DPrintf("%v %v don't vote for Candidate %v, already voted for Candidate %v", rf.state, rf.me, args.CandidateID, rf.votedFor)
		}
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
	for !rf.killed() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state == FOLLOWER {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			return
		}

		if reply.VoteGranted {
			rf.voteCount++
			DPrintf("%v %v got vote from %v", rf.state, rf.me, server)
			// (a) it wins the election
			if rf.voteCount > len(rf.peers)/2 && rf.state == CANDIDATE {
				DPrintf("%v %v got %v votes, win election with term %v", rf.state, rf.me, rf.voteCount, rf.currentTerm)
				rf.state = LEADER
				go rf.sendHeartBeat()
				return
			}
		}
		return
	}
}
func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	rf.initIndex() // Init nextIndex and matchIndex
	rf.voteCount = 1

	state, me, term := rf.state, rf.me, rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != me {
			go func(server int) {
				args := RequestVoteArgs{
					Term:         term,
					CandidateID:  me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}
				DPrintf("%v %v send vote request to %v, args: %+v", state, me, server, args)
				rf.sendRequestVote(server, &args, &reply)
			}(server)
		}
	}
}
