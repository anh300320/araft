package states

import (
	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/protocol"
)

type Candidate struct {
	raft       *raft.Raft
	transition chan raft.State
}

func (c *Candidate) Run() {
	return
}

func (c *Candidate) HandleHeartBeat(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	return protocol.AppendEntriesResponse{IsSucceeded: false}, nil
}

func (c *Candidate) HandleAppendEntries(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	return protocol.AppendEntriesResponse{IsSucceeded: false}, nil
}

func (c *Candidate) HandleVote(request protocol.VoteRequest) (protocol.VoteResponse, error) {
	return protocol.VoteResponse{}, nil
}

func (c *Candidate) HandlePreVote(request protocol.PreVoteRequest) (protocol.PreVoteResponse, error) {
	isNewTerm := c.raft.GetCurrentTerm() < request.HypotheticalTerm

	latestLogEntry := c.raft.GetLatestLogEntry()
	isLogUpToDate := latestLogEntry.Term < request.LastLogTerm ||
		(latestLogEntry.Term == request.LastLogTerm && latestLogEntry.Id <= request.LastLogIndex)

	return protocol.PreVoteResponse{
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
