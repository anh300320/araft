package raft

import (
	"github.com/anh300320/araft/internal/raft/protocol"
)

type State interface {
	Run()
	GetTransition() chan State

	HandleHeartBeat(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error)
	HandleAppendEntries(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error)
	HandleVote(request protocol.VoteRequest) (protocol.VoteResponse, error)
	HandlePreVote(request protocol.PreVoteRequest) (protocol.PreVoteResponse, error)
}

type TransitionSignal int

const (
	TransitionSignalCandidate TransitionSignal = iota
	TransitionSignalFollower
	TransitionSignalLeader
)
