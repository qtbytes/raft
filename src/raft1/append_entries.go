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

func (args RequestAppendArgs) String() string {
	return fmt.Sprintf("(Term:%d PrevLogIndex:%d PrevLogTerm:%d Entries(size:%d) LeaderCommit:%d)",
		args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
}

type RequestAppendReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm   int  // term in the conflicting entry (if any)
	XIndex  int  // index of first entry with that term (if any)
	XLen    int  // log length
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
	rf.persist()
	rf.state = FOLLOWER

	reply.Success = true
	reply.Term = rf.currentTerm

	DPrintf("%v %v received Entries (size: %v) from %v",
		rf.state, rf.me, len(args.Entries), args.LeaderID)

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if rf.len <= args.PrevLogIndex {
		DPrintf("%v %v reply false, len: %v <= prevLogIndex: %v",
			rf.state, rf.me, rf.len, args.PrevLogIndex)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XLen = rf.len
		return
	}

	entry := rf.get(args.PrevLogIndex)
	if entry.Term != args.PrevLogTerm {
		DPrintf("%v %v reply false, log %+v don't match (index: %v, term: %v)",
			rf.state, rf.me, entry, args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = entry.Term
		for _, entry := range rf.log {
			if entry.Term == reply.XTerm {
				reply.XIndex = entry.Index
				break
			}
		}
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it (§5.3)
	p := len(rf.log)

	for _, entry := range args.Entries {
		i := entry.Index
		if i < rf.len && rf.get(i).Term != entry.Term {
			p = min(i, p)
		}
	}

	if p != len(rf.log) {
		DPrintf("%v %v Log[%v] is conflict with new entry, delete log[%v:]", rf.state, rf.me, p, p)
		rf.len -= len(rf.log) - p
		rf.log = rf.log[:p]
		rf.persist()
	}

	// 4. Append any new entries not already in the log
	for _, entry := range args.Entries {
		i := entry.Index
		if i >= rf.len {
			DPrintf("%v %v append new entry %v to log", rf.state, rf.me, entry)
			rf.log = append(rf.log, entry)
			rf.len++
			rf.persist() // TODO: maybe should persist only once
		}
	}

	// 5. If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.len-1)
		DPrintf("%v %v update commitIndex to %v", rf.state, rf.me, rf.commitIndex)
		rf.needApply.Broadcast()
	}
	if len(args.Entries) > 0 { // ignore heartbeat(too many, influent debug)
		DPrintf("%v finish receiving Entries (size: %v) from %v", rf.me, len(args.Entries), args.LeaderID)
	}
}
func (rf *Raft) sendAppendEntries(server int, heartBeat bool) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		if !rf.isLeader() {
			return
		}

		rf.mu.Lock()
		currentTerm := rf.currentTerm
		var entries []LogEntry
		var prevLogIndex int
		var prevLogTerm int

		if heartBeat {
			prevLogIndex = rf.len - 1
			prevLogTerm = rf.getTerm(prevLogIndex)
		} else {
			nextIndex := rf.nextIndex[server]
			if nextIndex < rf.snapShotIndex {
				rf.mu.Unlock()
				rf.sendSanpShot(server)
				return
			} else {
				index := nextIndex - rf.snapShotIndex
				entries = rf.log[index:]
				prevLogIndex = nextIndex - 1
				prevLogTerm = rf.getTerm(prevLogIndex)
			}
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
		if !heartBeat {
			DPrintf("Leader %v sent args: %+v to Server %v", rf.me, args, server)
		}

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
			rf.switchToFollower(server, reply.Term)
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
			if rf.nextIndex[server] > 1 && rf.get(rf.nextIndex[server]-1).Term != reply.Term {
				rf.nextIndex[server]--
				DPrintf("%v %v receive failed from %v, accord reply.Term: %v, update nextIndex[%v] to %v",
					rf.state, rf.me, server, reply.Term, server, rf.nextIndex[server])
			}
			DPrintf("%v %v find appendEntries failed, retry with prevLogIndex: %v, args: %+v",
				rf.state, rf.me, args.PrevLogIndex, args)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) quickUpdateNextIndex(reply *RequestAppendReply, server int) {

	if reply.XTerm != 0 {
		xIndex := -1
		for i, entry := range rf.log {
			if entry.Term == reply.XTerm {
				xIndex = i
			}
		}
		if xIndex == -1 {
			// Case 1: leader doesn't have XTerm:
			rf.nextIndex[server] = reply.XIndex
		} else {
			// Case 2: leader has XTerm:
			// nextIndex = (index of leader's last entry for XTerm) + 1
			rf.nextIndex[server] = xIndex + 1
		}
	}
	// Case 3: follower's log is too short:
	if reply.XLen != 0 {
		rf.nextIndex[server] = reply.XLen
	}
	DPrintf("%v %v receive failed from %v, accord reply: %+v, update nextIndex[%v] to %v",
		rf.state, rf.me, server, reply, server, rf.nextIndex[server])
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
					rf.sendAppendEntries(server, true)
				}(server)
			}
		}
		time.Sleep(BROADCAST_TIME)
	}
}
