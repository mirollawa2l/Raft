package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

const electionPollInterval = 15 * time.Millisecond

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) != 0
}

// caller already holds rf.mu.
func (rf *Raft) resetElectionTimerLocked() {
	delay := time.Duration(
		300+rand.Intn(301),
	) * time.Millisecond

	rf.electionDeadline = time.Now().Add(delay)
}

// to follower
func (rf *Raft) stepDownLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}

	rf.role = Follower
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Check periodically, not continuously
		time.Sleep(electionPollInterval)

		rf.mu.Lock()

		due := rf.role != Leader &&
			!time.Now().Before(rf.electionDeadline)

		rf.mu.Unlock()

		if due && !rf.killed() {
			rf.startElection()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	// Another goroutine may have reset our timer
	if rf.killed() ||
		rf.role == Leader ||
		time.Now().Before(rf.electionDeadline) {

		rf.mu.Unlock()
		return
	}

	// 1. Become a candidate
	rf.role = Candidate

	// 2. Start a new term
	rf.currentTerm++
	term := rf.currentTerm

	// 3. Vote for ourselves
	rf.votedFor = rf.me

	// 4. Reset election timeout
	rf.resetElectionTimerLocked()

	// 5. Get the candidate's latest log information
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term

	// 6. Prepare the RequestVote RPC
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	// We already have our own vote
	votes := 1

	// Number of votes needed to win
	majority := len(rf.peers)/2 + 1

	// Handle a single-server cluster
	if votes >= majority {
		rf.role = Leader
		rf.onBecomeLeaderLocked()
		rf.mu.Unlock()
		return
	}

	// Never hold the mutex while waiting on RPCs
	rf.mu.Unlock()

	// 7. Ask every other server for its vote
	for peer := range rf.peers {

		if peer == rf.me {
			continue
		}

		go func(peer int) {
			var reply RequestVoteReply

			// Send RPC outside the mutex
			ok := rf.sendRequestVote(
				peer,
				&args,
				&reply,
			)

			// The request may have been lost
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Ignore results after shutdown
			if rf.killed() {
				return
			}

			// A newer term always takes priority
			if reply.Term > rf.currentTerm {
				rf.stepDownLocked(reply.Term)
				return
			}

			// Ignore replies from outdated elections
			if rf.role != Candidate ||
				rf.currentTerm != term {

				return
			}

			// Count only valid votes for this election term
			if reply.Term != term ||
				!reply.VoteGranted {

				return
			}

			// Vote count is protected by the mutex
			votes++

			// Majority reached
			if votes >= majority {
				rf.role = Leader

				// Person C initializes leader state
				rf.onBecomeLeaderLocked()
			}
		}(peer)
	}
}

// TEMPORARY: Remove when Person C implements this function.
func (rf *Raft) onBecomeLeaderLocked() {
	// Person C will initialize leader state
	// and arrange for heartbeats here.
}
