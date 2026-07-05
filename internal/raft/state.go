package raft

import (
	"github.com/anh300320/araft/internal/raft/protocol"
)

type State interface {
	Start() error
	Run()
	Close() error

	GetTransition() chan State
	HandleAppendEntries(request protocol.AppendEntriesRequest) (State, protocol.AppendEntriesResponse, error)
	HandleVote(request protocol.VoteRequest) (State, protocol.VoteResponse, error)
	HandlePreVote(request protocol.PreVoteRequest) (State, protocol.PreVoteResponse, error)
}
