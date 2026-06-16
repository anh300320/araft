package states

import (
	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
)

type Candidate struct {
	raft        *raft.Raft
	currentTerm common.Term
	transition  chan raft.State
}

func (c *Candidate) Run() {
	go c.raft.HandleMessage()
	return
}

func (c *Candidate) GetCurrentTerm() common.Term {
	return c.currentTerm
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
	return protocol.PreVoteResponse{}, nil
}

func (c *Candidate) GetTransition() chan raft.State {
	return c.transition
}
