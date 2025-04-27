package raft

import (
	"bytes"

	"6.5840/labgob"
)

type PersistState struct {
	Term     int
	VotedFor int
	Log      []LogEntry
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(PersistState{rf.currentTerm, rf.votedFor, rf.log[1:]})
	raftstate := w.Bytes()
	// DPrintf("3C: save state of server %v: term: %v, votedFor: %v, log: %v\n",
	// 	rf.me, rf.currentTerm, rf.votedFor, rf.log)
	rf.persister.Save(raftstate, rf.snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var ps PersistState
	if d.Decode(&ps) != nil {
		DPrintf("3C: Decode failed, something is wrong!\n")
	} else {
		rf.currentTerm = ps.Term
		rf.votedFor = ps.VotedFor
		rf.initLog(rf.snapShotTerm(), rf.snapShotIndex())
		rf.log = append(rf.log, ps.Log...)
		// DPrintf("3C: Recover state of server %v: term: %v, votedFor: %v, log: %v\n",
		// rf.me, ps.Term, ps.VotedFor, ps.Log)
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}
