package protocol

import "github.com/anh300320/araft/internal/raft/common"

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
	Term              common.Term
	MasterID          common.ServerID
	PrevLogIndex      common.LogIndex
	PrevLogTerm       common.Term
	LeaderCommitIndex common.LogIndex
	LogEntry          []common.LogEntry
}

type AppendEntriesResponse struct {
	Term        common.Term
	IsSucceeded bool

	LastLogIndex common.LogIndex
	LastLogTerm  common.Term
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

type ClientAppendEntryRequest struct {
	Data string
}

type ClientAppendEntryResponse struct {
	IsSucceeded bool
}

type Event int

const (
	EventAppendEntries Event = iota
	EventPreVote
	EventVote
	EventClientAppendEntry
)

type EventMessage struct {
	Event        Event
	Body         any
	ResponseChan chan any
}
