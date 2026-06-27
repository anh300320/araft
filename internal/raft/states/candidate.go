package states

import (
	"time"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/protocol"
	"go.uber.org/zap"
)

type Candidate struct {
	raft       *raft.Raft
	transition chan raft.State
}

func (c *Candidate) Run() {
	return
}

func (c *Candidate) HandleHeartBeat(request protocol.AppendEntriesRequest) (raft.State, protocol.AppendEntriesResponse, error) {
	return nil, protocol.AppendEntriesResponse{IsSucceeded: false}, nil
}

func (c *Candidate) HandleAppendEntries(request protocol.AppendEntriesRequest) (raft.State, protocol.AppendEntriesResponse, error) {
	return nil, protocol.AppendEntriesResponse{IsSucceeded: false}, nil
}

func (c *Candidate) HandleVote(request protocol.VoteRequest) (raft.State, protocol.VoteResponse, error) {
	if request.Term < c.raft.GetCurrentTerm() {
		return nil, protocol.VoteResponse{
			Term:        c.raft.GetCurrentTerm(),
			VoteGranted: false,
		}, nil
	}

	if request.Term == c.raft.GetCurrentTerm() {
		return nil, protocol.VoteResponse{
			Term:        c.raft.GetCurrentTerm(),
			VoteGranted: false,
		}, nil
	}

	if request.Term > c.raft.GetCurrentTerm() {
		nextState := &Follower{
			raft:            c.raft,
			lastHeartBeatAt: time.Now(),
			monitorInterval: 0,
			electionTimeout: 0,
			isRunning:       false,
			transition:      make(chan raft.State),
		}
		err := c.raft.UpgradeTerm(request.Term)
		if err != nil {
			c.raft.Logger.Error("failed to upgrade term", zap.Error(err))
			return nextState, protocol.VoteResponse{
				Term:        c.raft.GetCurrentTerm(),
				VoteGranted: false,
			}, err
		}
		return nextState, protocol.VoteResponse{}, nil
	}

	return nil, protocol.VoteResponse{
		Term:        c.raft.GetCurrentTerm(),
		VoteGranted: false,
	}, nil
}

func (c *Candidate) HandlePreVote(request protocol.PreVoteRequest) (raft.State, protocol.PreVoteResponse, error) {
	isNewTerm := c.raft.GetCurrentTerm() < request.HypotheticalTerm

	latestLogEntry := c.raft.GetLatestLogEntry()
	isLogUpToDate := latestLogEntry.Term < request.LastLogTerm ||
		(latestLogEntry.Term == request.LastLogTerm && latestLogEntry.Id <= request.LastLogIndex)

	return nil, protocol.PreVoteResponse{
		Term:    c.raft.GetCurrentTerm(),
		Granted: isNewTerm && isLogUpToDate,
	}, nil
}

func (c *Candidate) GetTransition() chan raft.State {
	return c.transition
}

func (c *Candidate) Close() error {
	close(c.transition)
	return nil
}
