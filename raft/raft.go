package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"

	"lab2raft/labrpc"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // Reserved for a later lab; ignore in Lab 2.
	Snapshot    []byte // Reserved for a later lab; ignore in Lab 2.
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	currentTerm int  // Which election term the server currently knows
	votedFor    int  // Which candidate received this server's vote in that term
	role        Role // Follower, candidate, or leader

	electionDeadline time.Time // The next time an election should begin if no valid heartbeat/vote reset occurs
	dead             int32     // Whether the Raft instance should stop its background work

	log         []LogEntry // Log entries
	commitIndex int        // Index of highest log entry known to be committed
	lastApplied int        // Index of highest log entry applied to state machine

	// Leader state (reinitialized after election)
	nextIndex  []int // For each server, index of the next log entry to send to that server
	matchIndex []int // For each server, index of highest log entry known to be replicated on server

	applyCh   chan ApplyMsg // Channel used to deliver committed commands
	applyCond *sync.Cond    // Wakes the applier when commitIndex moves

}

const heartbeatInterval = 120 * time.Millisecond

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//protects the shared state from concurrent modifications by election go routine
	//modifying current term/ role while tester calls get state
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm

	// A killed server should not report leadership
	isLeader := rf.role == Leader && !rf.killed()

	return term, isLeader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
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

	// Deny the vote by default
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// Ignore work after shutdown
	if rf.killed() {
		return
	}

	// Higher term: become follower
	if args.Term > rf.currentTerm {
		rf.stepDownLocked(args.Term)
	}

	// Reply with our latest term
	reply.Term = rf.currentTerm

	// Reject an outdated candidate
	if args.Term < rf.currentTerm {
		return
	}

	// Get our latest log entry
	myLastIndex := len(rf.log) - 1
	myLastTerm := rf.log[myLastIndex].Term

	// Candidate's log must be sufficiently up to date
	freshEnough := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm &&
			args.LastLogIndex >= myLastIndex)

	// Cannot vote for two different candidates in one term
	canVote := rf.votedFor == -1 ||
		rf.votedFor == args.CandidateID

	// Grant the vote only when both conditions hold
	if canVote && freshEnough {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true

		// Prevent immediately starting a competing election
		rf.resetElectionTimerLocked()
	}
}

type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // So follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store
	LeaderCommit int        // Leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // CurrentTerm, for leader to update itself
	Success       bool // True if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int  // Term of the conflicting entry if any (optimization)
	ConflictIndex int  // First index of that conflicting term in follower's log (optimization)

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Set default reply values
	reply.Success = false
	reply.Term = rf.currentTerm

	// Ignore RPCs if server is dead
	if rf.killed() {
		return
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownLocked(args.Term)
	}
	rf.role = Follower

	// Reset election timeout since we heard from a valid leader
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	lastLogIndex := len(rf.log) - 1
	if args.PrevLogIndex > lastLogIndex {
		reply.ConflictTerm = -1
		reply.ConflictIndex = lastLogIndex + 1
		return
	}

	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		// Find the first index of ConflictTerm to optimize backtracking
		conflictIndex := args.PrevLogIndex
		for conflictIndex > 0 && rf.log[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return
	}

	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term == entry.Term {
				continue
			}
			rf.log = rf.log[:idx]
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		newCommit := args.LeaderCommit
		if lastNewIndex < newCommit {
			newCommit = lastNewIndex
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Broadcast()
		}
	}
	reply.Success = true

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Only a live leader accepts commands
	if rf.killed() || rf.role != Leader {
		return -1, rf.currentTerm, false
	}

	// Add the command to our own log first
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	index := len(rf.log) - 1
	rf.matchIndex[rf.me] = index

	// With only one server there is nobody else to wait for
	rf.advanceCommitLocked()

	// Send it out now instead of waiting for the next heartbeat
	rf.broadcastLocked()

	return index, rf.currentTerm, true
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	//ensures safe access to the shutdown flag
	atomic.StoreInt32(&rf.dead, 1)

	// Wake up the applier so it can see that we are dead and exit.
	// We take the lock so the wake-up can't slip in between the
	// applier's killed() check and its Wait().
	if rf.applyCond != nil {
		rf.mu.Lock()
		rf.applyCond.Broadcast()
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (3A, 3B).
	// Initialize election state
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower

	// Dummy entry at log index zero
	rf.log = []LogEntry{{Term: 0}}

	// Save channel for the applier
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// Choose initial randomized election deadline
	rf.resetElectionTimerLocked()

	// Monitor elections concurrently
	go rf.electionTicker()

	// Hands committed entries to the service, in order
	go rf.applier()

	return rf
}

// Called (with rf.mu held) right after we win an election.
func (rf *Raft) onBecomeLeaderLocked() {
	n := len(rf.peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)

	// Optimistically assume everyone is caught up; the first rejected
	// AppendEntries will walk nextIndex back if they are not.
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1

	go rf.heartbeatLoop(rf.currentTerm)
}

// Keeps sending AppendEntries to everyone while we are the leader of this term.
func (rf *Raft) heartbeatLoop(term int) {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.broadcastLocked()
		rf.mu.Unlock()

		time.Sleep(heartbeatInterval)
	}
}

// Starts one sender goroutine per follower. Caller holds rf.mu.
func (rf *Raft) broadcastLocked() {
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.replicateTo(peer, rf.currentTerm)
		}
	}
}

// Sends one AppendEntries to a single follower and handles the answer.
// An empty Entries list is just a heartbeat.
func (rf *Raft) replicateTo(peer int, term int) {
	rf.mu.Lock()
	if rf.killed() || rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	next := rf.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	if next > len(rf.log) {
		next = len(rf.log)
	}
	prev := next - 1

	// Copy the entries, the log may change while the RPC is in flight
	entries := make([]LogEntry, len(rf.log)-next)
	copy(entries, rf.log[next:])

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.log[prev].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	// Never hold the lock during the RPC
	var reply AppendEntriesReply
	if !rf.sendAppendEntries(peer, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	// A newer term always wins, even if we are no longer leader
	if reply.Term > rf.currentTerm {
		rf.stepDownLocked(reply.Term)
		return
	}

	// Only then check that this reply belongs to our current leadership
	if rf.role != Leader || rf.currentTerm != term {
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.advanceCommitLocked()

		// Follower is still missing entries (new ones came in meanwhile)
		if rf.nextIndex[peer] < len(rf.log) {
			go rf.replicateTo(peer, term)
		}
		return
	}

	// Log mismatch. If nextIndex already moved since we sent this
	// request, the reply is out of date, so skip it.
	if rf.nextIndex[peer] != args.PrevLogIndex+1 {
		return
	}

	newNext := reply.ConflictIndex
	if reply.ConflictTerm != -1 {
		// If we also have entries from the conflicting term, retry just
		// after our last one. If not, skip that whole term.
		for i := len(rf.log) - 1; i >= 1; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				newNext = i + 1
				break
			}
			if rf.log[i].Term < reply.ConflictTerm {
				break
			}
		}
	}

	// Always move backwards, but never below 1
	if newNext >= rf.nextIndex[peer] {
		newNext = rf.nextIndex[peer] - 1
	}
	if newNext < 1 {
		newNext = 1
	}
	rf.nextIndex[peer] = newNext

	// Try again right away
	go rf.replicateTo(peer, term)
}

// Moves commitIndex forward if a majority has an entry. Only entries from
// the current term are counted (§5.4.2); older ones get committed along
// with them. Caller holds rf.mu.
func (rf *Raft) advanceCommitLocked() {
	if rf.role != Leader {
		return
	}
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			break
		}
		count := 1 // ourselves
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= n {
				count++
			}
		}
		if count*2 > len(rf.peers) {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			return
		}
	}
}

// Sends committed entries to the service, one at a time and in order.
// The channel send happens without the lock so we never block the RPC handlers.
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		var msgs []ApplyMsg
		for rf.lastApplied < rf.commitIndex && rf.lastApplied+1 < len(rf.log) {
			rf.lastApplied++
			msgs = append(msgs, ApplyMsg{
				Index:   rf.lastApplied,
				Command: rf.log[rf.lastApplied].Command,
			})
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			if !rf.sendApply(msg) {
				return
			}
		}
	}
}

// Sends on applyCh but gives up if we get killed while waiting.
func (rf *Raft) sendApply(msg ApplyMsg) bool {
	for {
		select {
		case rf.applyCh <- msg:
			return true
		case <-time.After(100 * time.Millisecond):
			if rf.killed() {
				return false
			}
		}
	}
}
