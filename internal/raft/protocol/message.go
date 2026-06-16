package protocol

import (
	"github.com/anh300320/araft/internal/raft/common"
)

type VoteRequest struct {
	Term         common.Term
	CandidateID  common.ServerID
	LastLogIndex common.LogIndex
	LastLogTerm  common.Term
}

type VoteResponse struct {
	Term        common.Term
	VoteGranted bool
}

type AppendEntriesRequest struct {
	MasterID     common.ServerID
	PrevLogIndex common.LogIndex
	PrevLogTerm  common.Term
	LogEntry     []common.LogEntry
}

type AppendEntriesResponse struct {
	IsSucceeded bool
}

type PreVoteRequest struct {
	Term         common.Term
	LastLogIndex common.LogIndex
	LastLogTerm  common.Term
}

type PreVoteResponse struct {
	IsSucceeded bool
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
