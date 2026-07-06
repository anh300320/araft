package raft

import (
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
)

type State interface {
	Run()
	Stop()
	IsRunning() bool

	GetTransition() chan *ChangeStateEvent
	HandleAppendEntries(request protocol.AppendEntriesRequest) (*ChangeStateEvent, protocol.AppendEntriesResponse, error)
	HandleVote(request protocol.VoteRequest) (*ChangeStateEvent, protocol.VoteResponse, error)
	HandlePreVote(request protocol.PreVoteRequest) (*ChangeStateEvent, protocol.PreVoteResponse, error)
}

type ChangeStateEvent struct {
	NextState State
	Term      common.Term
	VotedFor  common.ServerID
}
