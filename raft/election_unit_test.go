package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"lab2raft/labrpc"
)

// ==========================================
// HELPER: Create a Raft peer without
// automatically starting its election ticker
// ==========================================

func personANewPeer(
	term, votedFor int,
	role Role,
	logTerms ...int,
) *Raft {

	// Dummy log entry at index 0
	entries := []LogEntry{{Term: 0}}

	for _, entryTerm := range logTerms {
		entries = append(entries, LogEntry{
			Term: entryTerm,
		})
	}

	return &Raft{
		currentTerm: term,
		votedFor:    votedFor,
		role:        role,
		log:         entries,

		// Avoid automatic timeout during unit tests
		electionDeadline: time.Now().Add(2 * time.Hour),
	}
}

// ==========================================
// TEST 1: ALL REQUESTVOTE RULES
// ==========================================

func TestPersonA_VoteRules(t *testing.T) {
	cases := []struct {
		name                 string
		term, votedFor       int
		role                 Role
		logTerms             []int
		request              RequestVoteArgs
		grant                bool
		finalTerm, finalVote int
		finalRole            Role
		timerReset           bool
	}{
		{
			name: "First vote in higher term",
			term: 2, votedFor: -1, role: Follower,
			request: RequestVoteArgs{
				Term: 3, CandidateID: 1,
			},
			grant:     true,
			finalTerm: 3, finalVote: 1,
			finalRole: Follower, timerReset: true,
		},
		{
			name: "First vote in same term",
			term: 3, votedFor: -1, role: Follower,
			request: RequestVoteArgs{
				Term: 3, CandidateID: 1,
			},
			grant:     true,
			finalTerm: 3, finalVote: 1,
			finalRole: Follower, timerReset: true,
		},
		{
			name: "Retry by same candidate",
			term: 3, votedFor: 1, role: Follower,
			request: RequestVoteArgs{
				Term: 3, CandidateID: 1,
			},
			grant:     true,
			finalTerm: 3, finalVote: 1,
			finalRole: Follower, timerReset: true,
		},
		{
			name: "Reject different candidate same term",
			term: 3, votedFor: 1, role: Follower,
			request: RequestVoteArgs{
				Term: 3, CandidateID: 2,
			},
			grant:     false,
			finalTerm: 3, finalVote: 1,
			finalRole: Follower,
		},
		{
			name: "Reject lower term",
			term: 5, votedFor: 1, role: Follower,
			request: RequestVoteArgs{
				Term: 4, CandidateID: 2,
			},
			grant:     false,
			finalTerm: 5, finalVote: 1,
			finalRole: Follower,
		},
		{
			name: "Higher term with outdated log",
			term: 3, votedFor: 0, role: Candidate,
			logTerms: []int{2, 3, 3},
			request: RequestVoteArgs{
				Term: 4, CandidateID: 2,
				LastLogTerm: 2, LastLogIndex: 5,
			},
			grant:     false,
			finalTerm: 4, finalVote: -1,
			finalRole: Follower,
		},
		{
			name: "Higher term clears old vote",
			term: 2, votedFor: 0, role: Candidate,
			request: RequestVoteArgs{
				Term: 3, CandidateID: 2,
			},
			grant:     true,
			finalTerm: 3, finalVote: 2,
			finalRole: Follower, timerReset: true,
		},
		{
			name: "Reject older last log term even if longer",
			term: 5, votedFor: -1, role: Follower,
			logTerms: []int{2, 3, 3},
			request: RequestVoteArgs{
				Term: 5, CandidateID: 1,
				LastLogTerm: 2, LastLogIndex: 20,
			},
			grant:     false,
			finalTerm: 5, finalVote: -1,
			finalRole: Follower,
		},
		{
			name: "Accept newer last log term even if shorter",
			term: 5, votedFor: -1, role: Follower,
			logTerms: []int{2, 3, 3},
			request: RequestVoteArgs{
				Term: 5, CandidateID: 1,
				LastLogTerm: 4, LastLogIndex: 1,
			},
			grant:     true,
			finalTerm: 5, finalVote: 1,
			finalRole: Follower, timerReset: true,
		},
		{
			name: "Reject shorter log in equal last term",
			term: 5, votedFor: -1, role: Follower,
			logTerms: []int{2, 3, 3},
			request: RequestVoteArgs{
				Term: 5, CandidateID: 1,
				LastLogTerm: 3, LastLogIndex: 2,
			},
			grant:     false,
			finalTerm: 5, finalVote: -1,
			finalRole: Follower,
		},
		{
			name: "Accept equal last term and index",
			term: 5, votedFor: -1, role: Follower,
			logTerms: []int{2, 3, 3},
			request: RequestVoteArgs{
				Term: 5, CandidateID: 1,
				LastLogTerm: 3, LastLogIndex: 3,
			},
			grant:     true,
			finalTerm: 5, finalVote: 1,
			finalRole: Follower, timerReset: true,
		},
		{
			name: "Accept longer log in equal last term",
			term: 5, votedFor: -1, role: Follower,
			logTerms: []int{2, 3, 3},
			request: RequestVoteArgs{
				Term: 5, CandidateID: 1,
				LastLogTerm: 3, LastLogIndex: 4,
			},
			grant:     true,
			finalTerm: 5, finalVote: 1,
			finalRole: Follower, timerReset: true,
		},
		{
			name: "Leader rejects same-term rival",
			term: 5, votedFor: 0, role: Leader,
			request: RequestVoteArgs{
				Term: 5, CandidateID: 1,
			},
			grant:     false,
			finalTerm: 5, finalVote: 0,
			finalRole: Leader,
		},
		{
			name: "Leader steps down to higher term",
			term: 5, votedFor: 0, role: Leader,
			logTerms: []int{3, 4},
			request: RequestVoteArgs{
				Term: 6, CandidateID: 1,
				LastLogIndex: 0, LastLogTerm: 0,
			},
			grant:     false,
			finalTerm: 6, finalVote: -1,
			finalRole: Follower,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rf := personANewPeer(
				tc.term,
				tc.votedFor,
				tc.role,
				tc.logTerms...,
			)

			oldDeadline := rf.electionDeadline

			var reply RequestVoteReply
			rf.RequestVote(&tc.request, &reply)

			if reply.VoteGranted != tc.grant {
				t.Fatalf(
					"VoteGranted=%v, want %v",
					reply.VoteGranted, tc.grant,
				)
			}

			if reply.Term != tc.finalTerm ||
				rf.currentTerm != tc.finalTerm {
				t.Fatalf(
					"reply term=%d, local term=%d, want %d",
					reply.Term, rf.currentTerm, tc.finalTerm,
				)
			}

			if rf.votedFor != tc.finalVote ||
				rf.role != tc.finalRole {
				t.Fatalf(
					"votedFor=%d role=%v, want vote=%d role=%v",
					rf.votedFor, rf.role,
					tc.finalVote, tc.finalRole,
				)
			}

			if tc.timerReset {
				remaining := time.Until(rf.electionDeadline)

				if remaining < 250*time.Millisecond ||
					remaining > 700*time.Millisecond {
					t.Fatalf(
						"Election timer was not reset correctly: %s",
						remaining,
					)
				}
			} else if !rf.electionDeadline.Equal(oldDeadline) {
				t.Fatal("Rejected vote reset election timer")
			}
		})
	}
}

// ==========================================
// TEST 2: NO DOUBLE VOTING
// ==========================================

func TestPersonA_NoDoubleVotingSequence(t *testing.T) {
	rf := personANewPeer(3, -1, Follower)

	first := RequestVoteArgs{
		Term: 3, CandidateID: 1,
	}

	var r1 RequestVoteReply
	rf.RequestVote(&first, &r1)

	if !r1.VoteGranted {
		t.Fatal("First candidate should receive vote")
	}

	second := RequestVoteArgs{
		Term: 3, CandidateID: 2,
	}

	var r2 RequestVoteReply
	rf.RequestVote(&second, &r2)

	if r2.VoteGranted {
		t.Fatal("Server voted twice in same term")
	}

	var retry RequestVoteReply
	rf.RequestVote(&first, &retry)

	if !retry.VoteGranted || rf.votedFor != 1 {
		t.Fatal("Original candidate retry must succeed")
	}
}

// ==========================================
// TEST 3: CONCURRENT VOTE REQUESTS
// ==========================================

func TestPersonA_ConcurrentVoting(t *testing.T) {
	rf := personANewPeer(6, -1, Follower)

	const candidates = 60

	grants := make(chan int, candidates)
	var wg sync.WaitGroup

	for id := 0; id < candidates; id++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			args := RequestVoteArgs{
				Term: 7, CandidateID: id,
			}

			var reply RequestVoteReply
			rf.RequestVote(&args, &reply)

			if reply.VoteGranted {
				grants <- id
			}
		}(id)
	}

	wg.Wait()
	close(grants)

	count := 0
	winner := -1

	for id := range grants {
		count++
		winner = id
	}

	if count != 1 {
		t.Fatalf(
			"Granted %d votes, expected exactly 1",
			count,
		)
	}

	if rf.currentTerm != 7 ||
		rf.votedFor != winner {
		t.Fatal("Voting state inconsistent")
	}
}

// ==========================================
// TEST 4: STEP DOWN CORRECTLY
// ==========================================

func TestPersonA_StepDownLocked(t *testing.T) {
	rf := personANewPeer(4, 2, Leader)

	oldDeadline := rf.electionDeadline

	rf.mu.Lock()
	rf.stepDownLocked(4)
	rf.mu.Unlock()

	if rf.role != Follower ||
		rf.currentTerm != 4 ||
		rf.votedFor != 2 {
		t.Fatal("Same-term step-down must preserve vote")
	}

	rf.mu.Lock()
	rf.stepDownLocked(8)
	rf.mu.Unlock()

	if rf.role != Follower ||
		rf.currentTerm != 8 ||
		rf.votedFor != -1 {
		t.Fatal("Higher term must reset vote and step down")
	}

	if !rf.electionDeadline.Equal(oldDeadline) {
		t.Fatal("stepDownLocked reset election timer")
	}
}

// ==========================================
// TEST 5: ELECTION TIMER RANGE
// ==========================================

func TestPersonA_TimerRange(t *testing.T) {
	rf := personANewPeer(0, -1, Follower)

	for i := 0; i < 20; i++ {
		rf.mu.Lock()
		rf.resetElectionTimerLocked()
		remaining := time.Until(rf.electionDeadline)
		rf.mu.Unlock()

		if remaining < 250*time.Millisecond ||
			remaining > 700*time.Millisecond {
			t.Fatalf(
				"Election timeout outside expected range: %s",
				remaining,
			)
		}
	}
}

// ==========================================
// TEST 6: GETSTATE AND KILL
// ==========================================

func TestPersonA_GetStateAndKill(t *testing.T) {
	rf := personANewPeer(9, 0, Leader)

	term, leader := rf.GetState()

	if term != 9 || !leader {
		t.Fatal("GetState returned incorrect leader state")
	}

	rf.Kill()
	rf.Kill() // Calling twice should be safe

	if !rf.killed() {
		t.Fatal("Server should be marked dead")
	}

	term, leader = rf.GetState()

	if term != 9 || leader {
		t.Fatal("Killed server should not claim leadership")
	}
}

// ==========================================
// TEST 7: DEAD SERVER REJECTS VOTES
// ==========================================

func TestPersonA_DeadServerCannotGrantVote(t *testing.T) {
	rf := personANewPeer(2, -1, Follower)
	rf.Kill()

	args := RequestVoteArgs{
		Term: 3, CandidateID: 1,
	}

	var reply RequestVoteReply
	rf.RequestVote(&args, &reply)

	if reply.VoteGranted ||
		reply.Term != 2 ||
		rf.currentTerm != 2 {
		t.Fatal("Dead server processed a vote incorrectly")
	}
}

// ==========================================
// TEST 8: ELECTION GUARDS
// ==========================================

func TestPersonA_ElectionGuard(t *testing.T) {
	rf := personANewPeer(2, -1, Follower)

	rf.peers = make([]*labrpc.ClientEnd, 3)

	// Deadline has not expired
	rf.electionDeadline = time.Now().Add(time.Hour)
	rf.startElection()

	if rf.role != Follower ||
		rf.currentTerm != 2 ||
		rf.votedFor != -1 {
		t.Fatal("Election started too early")
	}

	// Dead server must not start elections
	rf.electionDeadline = time.Now().Add(-time.Second)
	rf.Kill()
	rf.startElection()

	if rf.role != Follower || rf.currentTerm != 2 {
		t.Fatal("Dead server started an election")
	}
}

// ==========================================
// TEST 9: SINGLE-SERVER MAJORITY
// ==========================================

func TestPersonA_SinglePeerElection(t *testing.T) {
	rf := personANewPeer(0, -1, Follower)

	rf.me = 0
	rf.peers = make([]*labrpc.ClientEnd, 1)
	rf.electionDeadline = time.Now().Add(-time.Second)

	rf.startElection()

	term, leader := rf.GetState()

	if term != 1 || !leader || rf.votedFor != 0 {
		t.Fatal("Single server did not elect itself")
	}

	rf.Kill()
}

// ==========================================
// NETWORK TESTING HELPERS
// ==========================================

// Build real labrpc connections without starting
// election tickers for the follower servers.

func personATestNetwork(n int) (*labrpc.Network, []*Raft) {
	network := labrpc.MakeNetwork()
	servers := make([]*Raft, n)

	for i := 0; i < n; i++ {
		rf := personANewPeer(0, -1, Follower)
		rf.me = i
		servers[i] = rf

		srv := labrpc.MakeServer()
		srv.AddService(labrpc.MakeService(rf))
		network.AddServer(i, srv)
	}

	peers := make([]*labrpc.ClientEnd, n)

	for i := 0; i < n; i++ {
		name := fmt.Sprintf("candidate0-to-%d", i)

		peers[i] = network.MakeEnd(name)
		network.Connect(name, i)
		network.Enable(name, true)
	}

	servers[0].peers = peers

	return network, servers
}

// Wait for a condition without waiting forever.

func personAWaitFor(
	t *testing.T,
	timeout time.Duration,
	check func() bool,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatal("Timed out waiting for election state")
}

// ==========================================
// TEST 10: MAJORITY ELECTION OVER REAL RPC
// ==========================================

func TestPersonA_MajorityElectionViaRPC(t *testing.T) {
	_, servers := personATestNetwork(3)

	for _, rf := range servers {
		t.Cleanup(rf.Kill)
	}

	candidate := servers[0]
	candidate.electionDeadline = time.Now().Add(-time.Second)

	candidate.startElection()

	personAWaitFor(t, time.Second, func() bool {
		term, leader := candidate.GetState()
		return term == 1 && leader
	})

	candidate.mu.Lock()
	defer candidate.mu.Unlock()

	if candidate.votedFor != 0 {
		t.Fatal("Candidate did not vote for itself")
	}
}

// ==========================================
// TEST 11: NO MAJORITY, NO LEADER
// ==========================================

func TestPersonA_NoMajorityNoLeader(t *testing.T) {
	network, servers := personATestNetwork(5)

	for _, rf := range servers {
		t.Cleanup(rf.Kill)
	}

	// Disconnect 3 of the 4 other peers
	for i := 2; i < 5; i++ {
		network.Enable(
			fmt.Sprintf("candidate0-to-%d", i),
			false,
		)
	}

	candidate := servers[0]
	candidate.electionDeadline = time.Now().Add(-time.Second)

	candidate.startElection()

	time.Sleep(250 * time.Millisecond)

	candidate.mu.Lock()
	defer candidate.mu.Unlock()

	if candidate.currentTerm != 1 ||
		candidate.role != Candidate {
		t.Fatal("Candidate changed state without majority")
	}
}

// ==========================================
// TEST 12: HIGHER-TERM RPC RESPONSE
// ==========================================

func TestPersonA_HigherTermRPCForcesStepDown(t *testing.T) {
	network, servers := personATestNetwork(3)

	for _, rf := range servers {
		t.Cleanup(rf.Kill)
	}

	// Peer 1 has already discovered term 9
	servers[1].currentTerm = 9

	// Disable the connection to peer 2
	network.Enable("candidate0-to-2", false)

	candidate := servers[0]
	candidate.electionDeadline = time.Now().Add(-time.Second)

	candidate.startElection()

	personAWaitFor(t, time.Second, func() bool {
		term, leader := candidate.GetState()
		return term == 9 && !leader
	})

	candidate.mu.Lock()
	defer candidate.mu.Unlock()

	if candidate.role != Follower ||
		candidate.votedFor != -1 {
		t.Fatal("Candidate failed to step down")
	}
}

// ==========================================
// TEST 13: ELECTION TICKER
// ==========================================

func TestPersonA_TickerElectsSinglePeer(t *testing.T) {
	rf := Make(
		make([]*labrpc.ClientEnd, 1),
		0,
		make(chan ApplyMsg),
	)

	t.Cleanup(rf.Kill)

	personAWaitFor(t, 2*time.Second, func() bool {
		_, leader := rf.GetState()
		return leader
	})
}

// ==========================================
// TEST 14: RETRY AFTER SPLIT ELECTION
// ==========================================

func TestPersonA_RetryAfterElectionTimeout(t *testing.T) {
	network, servers := personATestNetwork(5)

	for _, rf := range servers {
		t.Cleanup(rf.Kill)
	}

	// Prevent every outgoing vote request
	for i := 1; i < 5; i++ {
		network.Enable(
			fmt.Sprintf("candidate0-to-%d", i),
			false,
		)
	}

	candidate := servers[0]
	candidate.electionDeadline = time.Now().Add(-time.Second)

	// First election: term 1
	candidate.startElection()

	candidate.mu.Lock()
	firstTerm := candidate.currentTerm
	candidate.electionDeadline = time.Now().Add(-time.Second)
	candidate.mu.Unlock()

	// Second election: term 2
	candidate.startElection()

	candidate.mu.Lock()
	defer candidate.mu.Unlock()

	if firstTerm != 1 ||
		candidate.currentTerm != 2 ||
		candidate.role != Candidate ||
		candidate.votedFor != 0 {
		t.Fatal("Candidate failed to retry in new term")
	}
}

// ==========================================
// TEST 15: IGNORE DELAYED OLD VOTES
// ==========================================

func TestPersonA_StaleVoteRepliesIgnored(t *testing.T) {
	network, servers := personATestNetwork(3)

	for _, rf := range servers {
		t.Cleanup(rf.Kill)
	}

	// Temporarily block vote processing
	servers[1].mu.Lock()
	servers[2].mu.Lock()

	candidate := servers[0]
	candidate.electionDeadline = time.Now().Add(-time.Second)

	// Election in term 1
	candidate.startElection()

	// Ensure requests have reached both RPC servers
	personAWaitFor(t, time.Second, func() bool {
		return network.GetCount(1) > 0 &&
			network.GetCount(2) > 0
	})

	// Simulate discovering a newer term
	candidate.mu.Lock()
	candidate.currentTerm = 2
	candidate.role = Follower
	candidate.votedFor = -1
	candidate.mu.Unlock()

	// Release the delayed vote requests
	servers[1].mu.Unlock()
	servers[2].mu.Unlock()

	time.Sleep(100 * time.Millisecond)

	term, leader := candidate.GetState()

	if term != 2 || leader {
		t.Fatal("Old election votes incorrectly elected leader")
	}
}
