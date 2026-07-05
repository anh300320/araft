package states

import (
	"sync"
	"time"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
	"go.uber.org/zap"
)

type Candidate struct {
	raft       *raft.Raft
	transition chan raft.State
}

func (c *Candidate) Start() error {
	return c.raft.SetVotedFor(c.raft.GetServerID())
}

func (c *Candidate) Run() {
	responses := c.sendVoteRequests()
	var nextState raft.State
	if c.promoteToMaster(responses) {
		nextState = &Master{
			raft:       c.raft,
			nextIndex:  0,
			matchIndex: 0,
			transition: make(chan raft.State),
		}
	} else {
		c.raft.UpgradeTerm(c.raft.GetCurrentTerm() + 1)
		nextState = &Candidate{
			raft:       c.raft,
			transition: make(chan raft.State),
		}
	}
	c.transition <- nextState
}

func (c *Candidate) sendVoteRequests() chan protocol.VoteResponse {
	lastLogEntry := c.raft.GetLatestLogEntry()
	req := protocol.VoteRequest{
		CandidateID:  c.raft.GetServerID(),
		Term:         c.raft.GetCurrentTerm(),
		LastLogIndex: lastLogEntry.Id,
		LastLogTerm:  lastLogEntry.Term,
	}

	peers := c.raft.GetOthers()
	responses := make(chan protocol.VoteResponse, len(peers))
	var wg sync.WaitGroup
	for _, peer := range peers {
		t := c.raft.GetTransport()
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := t.SendVote(peer, req)
			if err != nil {
				return
			}
			responses <- resp
		}()
	}

	go func() {
		wg.Wait()
		close(responses)
	}()

	return responses
}

func (c *Candidate) promoteToMaster(responses chan protocol.VoteResponse) bool {
	grantedCount := 0
	receivedCount := 0
	electionTimer := time.NewTimer(c.raft.RandomElectionTimeout())
	for {
		select {
		case resp := <-responses:
			receivedCount += 1
			if receivedCount > len(responses) {
				return false
			}
			if resp.VoteGranted {
				grantedCount += 1
				if grantedCount >= common.GetMajorityCount(len(responses)) {
					return true
				}
			}
		case <-electionTimer.C:
			return false
		}
	}
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
			isRunning:       false,
			transition:      make(chan raft.State),
			timerResetEvent: make(chan struct{}),
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
