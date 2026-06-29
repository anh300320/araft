package protocol

import (
	"github.com/anh300320/araft/internal/raft/common"
)

type VoteRequest struct {
	CandidateID  common.ServerID
	Term         common.Term
	LastLogIndex common.LogIndex
	LastLogTerm  common.Term
}

type VoteResponse struct {
	Term        common.Term
	VoteGranted bool
}

type AppendEntriesRequest struct {
	MasterID          common.ServerID
	PrevLogIndex      common.LogIndex
	PrevLogTerm       common.Term
	LeaderCommitIndex common.LogIndex
	LogEntry          []common.LogEntry
}

type AppendEntriesResponse struct {
	IsSucceeded bool
}

type PreVoteRequest struct {
	HypotheticalTerm common.Term
	serverID         common.ServerID
	LastLogIndex     common.LogIndex
	LastLogTerm      common.Term
	CommitIndex      common.LogIndex
}

type PreVoteResponse struct {
	Term    common.Term
	Granted bool
}

type Event int

const (
	EventHeartBeat Event = iota
	EventAppendEntries
	EventPreVote
	EventVote
)

type EventMessage struct {
	Event        Event
	Body         any
	ResponseChan chan any
}
