package raft

import (
	"fmt"
	"time"

	"6.5840/raftapi"
)

type LogEntry struct {
	Term  int
	Index int
	Entry raftapi.ApplyMsg
}

func (log LogEntry) String() string {
	return fmt.Sprintf("(term: %v, index: %v, command: %v)",
		log.Term, log.Index, log.Entry.Command)
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
		DPrintf("%v %v reject AppendEntries from %v, because term: %v > %v", rf.state, rf.me,
			args.LeaderID, rf.currentTerm, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.resetElectionTimer()
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.state = FOLLOWER

	reply.Success = true
	reply.Term = rf.currentTerm

	DPrintf("%v %v received Entries (size: %v) from %v",
		rf.state, rf.me, len(args.Entries), args.LeaderID)

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if len(rf.log) <= args.PrevLogIndex {
		DPrintf("%v %v reply false, len: %v < prevLogIndex: %v",
			rf.state, rf.me, len(rf.log), args.PrevLogIndex)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%v %v reply false, log %+v don't match (index: %v, term: %v)",
			rf.state, rf.me, rf.log[args.PrevLogIndex], args.PrevLogIndex, args.PrevLogTerm)
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

	if p != len(rf.log) {
		DPrintf("%v %v Log[%v] is conflict with new entry, delete log[%v:]", rf.state, rf.me, p, p)
		rf.log = rf.log[:p]
	}

	// 4. Append any new entries not already in the log
	for _, entry := range args.Entries {
		i := entry.Index
		if i >= len(rf.log) {
			DPrintf("%v %v append new entry (term: %v, index: %v, command: %v) to log",
				rf.state, rf.me, entry.Term, entry.Index, entry.Entry.Command)
			DPrintf("%v %v append new entry %v to log", rf.state, rf.me, entry)
			rf.log = append(rf.log, entry)
		}
	}

	// 5. If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		DPrintf("%v %v update commitIndex to %v", rf.state, rf.me, rf.commitIndex)
		rf.needApply.Broadcast()
	}

}
func (rf *Raft) sendAppendEntries(server int, heartBeat bool) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		isLeader := rf.state == LEADER
		if !isLeader {
			rf.mu.Unlock()
			return
		}

		currentTerm := rf.currentTerm
		nextIndex := rf.nextIndex[server]
		entries := []LogEntry{}
		prevLogIndex := len(rf.log) - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		if !heartBeat {
			entries = rf.log[nextIndex:]
			prevLogIndex = nextIndex - 1
			prevLogTerm = rf.log[prevLogIndex].Term
		}
		leaderCommit := rf.commitIndex
		rf.mu.Unlock()

		args := RequestAppendArgs{
			Term:         currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}
		reply := RequestAppendReply{}
		DPrintf("%v sent args: %+v to %v", rf.me, args, server)

		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		if !ok {
			continue
		}
		rf.mu.Lock()
		term := rf.currentTerm
		state := rf.state
		rf.mu.Unlock()

		//  old RPC replies
		if term != args.Term || state != LEADER {
			DPrintf("%v %v find currentTerm: %v != args.Term: %v, ignore old rpc replies, args: %+v",
				state, rf.me, term, args.Term, args)
			return
		}
		if reply.Term > term {
			rf.mu.Lock()
			DPrintf("%v %v find server %v has larger Trem, switch to Follower", rf.state, rf.me, server)
			rf.state = FOLLOWER
			rf.resetElectionTimer()
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			rf.mu.Lock()
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			if len(args.Entries) > 0 {
				DPrintf("%v %v receive success from Follower %v, update matchIndex to %v, nextIndex to %v",
					rf.state, rf.me, server, rf.matchIndex[server], rf.nextIndex[server])
			}
			rf.mu.Unlock()
			return
		} else {
			rf.mu.Lock()
			if rf.nextIndex[server] > 1 && rf.log[rf.nextIndex[server]-1].Term != reply.Term {
				rf.nextIndex[server]--
				DPrintf("%v %v receive failed from %v, accord reply.Term: %v, update nextIndex[%v] to %v",
					rf.state, rf.me, server, reply.Term, server, rf.nextIndex[server])
			}
			// DPrintf("%v %v find appendEntries failed, retry with prevLogIndex: %v, args: %+v",
			// 	rf.state, rf.me, args.PrevLogIndex, args)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	for !rf.killed() {

		rf.mu.Lock()
		isLeader := rf.state == LEADER
		state, me := rf.state, rf.me
		rf.mu.Unlock()

		if !isLeader {
			DPrintf("%v %v is not Leader anymore, stop send heartbeat", state, me)
			return
		}

		for server := range rf.peers {
			if server != rf.me {
				go func(server int) {
					// DPrintf("%v %v send heartbeat to %v, %+v", rf.state, rf.me, server, args)
					rf.sendAppendEntries(server, true)
				}(server)
			}
		}
		time.Sleep(BROADCAST_TIME)
	}
}
