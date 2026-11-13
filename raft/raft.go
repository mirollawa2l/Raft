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

	currentTerm int  //Which election term the server currently knows
	votedFor    int  //Which candidate received this server's vote in that term
	role        Role //Follower, candidate, or leader

	electionDeadline time.Time //The next time an election should begin if no valid heartbeat/vote reset occurs
	dead             int32     //Whether the Raft instance should stop its background work

	log     []LogEntry    //Entries used for candidate log-freshness checks
	applyCh chan ApplyMsg //Channel used by Person C to deliver committed commands
	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

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
	// Your data here (3A, 3B).
}

type AppendEntriesReply struct {
	// Your data here (3A, 3B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
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
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	//ensures safe access to the shutdown flag
	atomic.StoreInt32(&rf.dead, 1)
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

	// Save channel for Person C's applier
	rf.applyCh = applyCh

	// Choose initial randomized election deadline
	rf.resetElectionTimerLocked()

	// Monitor elections concurrently
	go rf.electionTicker()

	// Person C adds leader/applier setup as required

	return rf
}
