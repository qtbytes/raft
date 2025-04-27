package raft

import (
	"time"

	"6.5840/raftapi"
)

type SnapShotArgs struct {
	Term              int // leader’s term
	LeaderId          int // so follower can redirect clients
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int // term of lastIncludedIndex
	// Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data []byte // raw bytes of the snapshot chunk, starting at offset
	// Done bool   // true if this is the last chunk
}

type SnapShotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) SaveSnapShot(snapshot []byte, term, index int) {
	DPrintf("Saving snapshot: %v, term: %v, index: %v", snapshot, term, index)

	rf.applyCh <- raftapi.ApplyMsg{SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  term,
		SnapshotIndex: index}
}

func (rf *Raft) InstallSanpShot(args *SnapShotArgs, reply *SnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// Send the entire snapshot in a single InstallSnapshot RPC.
	// Don't implement Figure 13's offset mechanism for splitting up the snapshot.

	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	// 4. Reply and wait for more data chunks if done is false
	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index

	if args.LastIncludedIndex > rf.snapShotIndex() {
		rf.log[0] = LogEntry{Term: args.LastIncludedIndex, Index: args.LastIncludedTerm}
		DPrintf("update snapshot index to %v", rf.snapShotIndex())
		copy(rf.snapShot, args.Data)
	}

	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	for i, entry := range rf.log {
		if entry.Index == args.LastIncludedIndex && entry.Term == args.LastIncludedTerm {
			DPrintf("remove log index < snapshot's last included entry")
			rf.log = append(rf.log[0:1], rf.log[i:]...)
			return
		}
	}
	// 7. Discard the entire log
	rf.log = rf.log[:1]
	// 8. Reset state machine using snapshot contents (and load
	// snapshot’s cluster configuration)
	rf.SaveSnapShot(rf.snapShot, args.LastIncludedTerm, args.LastIncludedIndex)
}

func (rf *Raft) sendSanpShot(server int) {
	rf.mu.Lock()
	args := SnapShotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapShotIndex(),
		LastIncludedTerm:  rf.snapShotTerm(),
		Data:              rf.snapShot,
	}
	rf.mu.Unlock()
	reply := SnapShotReply{}

	for {
		DPrintf("Sending snapshot to %v", server)
		ok := rf.peers[server].Call("Raft.InstallSanpShot", &args, &reply)
		if !ok {
			time.Sleep(BROADCAST_TIME)
			continue
		}

		if reply.Term > rf.currentTerm {
			rf.switchToFollower(server, reply.Term)
		}
		break
	}
}
