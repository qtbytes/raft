package raft

import (
	"time"

	"6.5840/raftapi"
)

type LogEntry struct {
	Term  int
	Index int
	Entry raftapi.ApplyMsg
}

type RequestAppendArgs struct {
	Term         int        // leader’s term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex

}
type RequestAppendReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

}

func (rf *Raft) AppendEntries(args *RequestAppendArgs, reply *RequestAppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		DPrintf("%v %v has term: %v, reject AppendEntries from %v with term %v", rf.state, rf.me, rf.currentTerm, args.LeaderID, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	DPrintf("%v %v has term: %v, received AppendEntries from %v with term %v", rf.state, rf.me, rf.currentTerm, args.LeaderID, args.Term)
	rf.resetElectionTimer()
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.state = FOLLOWER
	reply.Success = true
	reply.Term = rf.currentTerm

	// heartbeat
	if len(args.Entries) == 0 {
		return
	}
	// Rules for Servers:
	// If commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine (§5.3)
	for args.LeaderCommit > rf.lastApplied {
		rf.applyCh <- rf.log[rf.lastApplied].Entry
		rf.lastApplied++
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if len(rf.log) < args.PrevLogIndex || (len(rf.log) > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it (§5.3)
	p := len(rf.log)

	for _, entry := range args.Entries {
		i := entry.Index
		if i < len(rf.log) && rf.log[i].Term != entry.Term {
			p = min(i, p)
		}
	}

	rf.log = rf.log[:p]

	// 4. Append any new entries not already in the log
	for _, entry := range args.Entries {
		i := entry.Index
		if i >= len(rf.log) {
			rf.log = append(rf.log, entry)
		}
	}

	// 5. If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
	}

}
func (rf *Raft) sendAppendEntries(server int, args *RequestAppendArgs, reply *RequestAppendReply) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		DPrintf("%v %v send heartbeat to %v", rf.state, rf.me, server)
		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//  old RPC replies
		if rf.currentTerm != args.Term {
			DPrintf("%v %v find %v != %v, maybe old rpc replies", rf.state, rf.me, rf.currentTerm, args.Term)
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			return
		} else if !reply.Success {
			rf.nextIndex[server]--
			// time.Sleep(10 * time.Millisecond)
			// continue
		}
		return
	}
}
func (rf *Raft) sendHeartBeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		} else {
			rf.mu.Unlock()
		}
		for server := range rf.peers {
			if server != rf.me {
				go func(server int) {
					rf.mu.Lock()
					args := RequestAppendArgs{
						LeaderID: rf.me,
						Term:     rf.currentTerm,
						Entries:  []LogEntry{},
						// LeaderCommit: rf.commitIndex,
					}
					reply := RequestAppendReply{}
					rf.mu.Unlock()
					rf.sendAppendEntries(server, &args, &reply)
				}(server)
			}
		}
		time.Sleep(BROADCAST_TIME)
	}
}
