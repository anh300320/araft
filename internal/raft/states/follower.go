package states

import (
	"time"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
)

type Follower struct {
	raft *raft.Raft

	lastHeartBeatAt time.Time

	monitorInterval time.Duration
	electionTimeout time.Duration
	transition      chan raft.State
}

func (f *Follower) Run() {
	go f.raft.HandleMessage()
	go f.monitorHeartBeat()
}

func (f *Follower) GetTransition() chan raft.State {
	return f.transition
}

func (f *Follower) monitorHeartBeat() {
	for {
		if time.Now().Sub(f.lastHeartBeatAt) > f.electionTimeout {
			prevCandidateState := &PreCandidate{
				NextTerm:     f.raft.GetCurrentTerm(),
				LastLogIndex: 0,
				LastLogTerm:  0,
				others:       f.raft.GetOthers(),
				transition:   make(chan raft.State),
			}
			f.transition <- prevCandidateState
			defer close(f.transition)
		}
		time.Sleep(f.monitorInterval)
	}
}

func (f *Follower) HandleHeartBeat(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	f.lastHeartBeatAt = time.Now()
	return protocol.AppendEntriesResponse{IsSucceeded: true}, nil
}

func (f *Follower) HandleAppendEntries(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	return protocol.AppendEntriesResponse{IsSucceeded: true}, nil
}

func (f *Follower) HandleVote(request protocol.VoteRequest) (protocol.VoteResponse, error) {
	return protocol.VoteResponse{
		Term:        f.raft.GetCurrentTerm(),
		VoteGranted: false,
	}, nil
}

func (f *Follower) HandlePreVote(request protocol.PreVoteRequest) (protocol.PreVoteResponse, error) {
	if f.raft.GetCurrentTerm() >= request.Term {
		return protocol.PreVoteResponse{IsSucceeded: false}, nil
	}
	return protocol.PreVoteResponse{IsSucceeded: true}, nil
}

func (f *Follower) GetCurrentTerm() common.Term {
	return f.raft.GetCurrentTerm()
}
