package raft

import (
	"github.com/anh300320/araft/internal/raft/protocol"
)

type State interface {
	Start() error
	Run()
	Close() error

	GetTransition() chan State
	HandleHeartBeat(request protocol.AppendEntriesRequest) (State, protocol.AppendEntriesResponse, error)
	HandleAppendEntries(request protocol.AppendEntriesRequest) (State, protocol.AppendEntriesResponse, error)
	HandleVote(request protocol.VoteRequest) (State, protocol.VoteResponse, error)
	HandlePreVote(request protocol.PreVoteRequest) (State, protocol.PreVoteResponse, error)
}

type TransitionSignal int

const (
	TransitionSignalCandidate TransitionSignal = iota
	TransitionSignalFollower
	TransitionSignalLeader
)
