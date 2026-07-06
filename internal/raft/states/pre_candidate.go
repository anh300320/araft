package states

import (
	"sync"
	"time"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
	"go.uber.org/zap"
)

type PreCandidate struct {
	raft *raft.Raft

	LastLogIndex common.LogIndex
	LastLogTerm  common.Term

	isRunning bool

	transition chan *raft.ChangeStateEvent
	stopSignal chan struct{}
}

func (p *PreCandidate) Run() {
	defer close(p.transition)
	defer func() {
		p.isRunning = false
	}()
	p.isRunning = true

	responses := make(chan protocol.PreVoteResponse, len(p.raft.GetOthers()))

	p.sendPreVoteRequests(responses)
	var nextState raft.State
	nextTerm := p.raft.GetCurrentTerm()
	if p.promoteToCandidate(responses) {
		nextState = &Candidate{
			raft:       p.raft,
			transition: make(chan *raft.ChangeStateEvent),
		}
		nextTerm += 1
	} else {
		nextState = &Follower{
			raft:            p.raft,
			isRunning:       false,
			transition:      make(chan *raft.ChangeStateEvent),
			timerResetEvent: make(chan struct{}),
		}
	}
	p.transition <- &raft.ChangeStateEvent{
		NextState: nextState,
		Term:      nextTerm,
		VotedFor:  p.raft.GetServerID(),
	}
	<-p.stopSignal
}

func (p *PreCandidate) IsRunning() bool {
	return p.isRunning
}

func (p *PreCandidate) GetTransition() chan *raft.ChangeStateEvent {
	return p.transition
}

func (p *PreCandidate) sendPreVoteRequests(responses chan protocol.PreVoteResponse) {
	var wg sync.WaitGroup
	for _, other := range p.raft.GetOthers() {
		request := protocol.PreVoteRequest{
			HypotheticalTerm: p.getHypotheticalTerm(),
			LastLogIndex:     p.LastLogIndex,
			LastLogTerm:      p.LastLogTerm,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := p.raft.GetTransport()
			response, err := t.SendPreVote(other, request)
			if err != nil {
				p.raft.Logger.Error(
					"failed to send pre-vote to",
					zap.String("address", other.GetAddress()),
				)
			} else {
				responses <- response
			}
		}()
	}

	go func() {
		wg.Wait()
		close(responses)
	}()
}

func (p *PreCandidate) promoteToCandidate(responses chan protocol.PreVoteResponse) bool {
	successCount := 0
	receivedCount := 0
	electionTimer := time.NewTimer(p.raft.RandomElectionTimeout())
	for {
		select {
		case resp := <-responses:
			receivedCount += 1
			if receivedCount > len(responses) {
				return false
			}
			if resp.Granted {
				successCount += 1
				if successCount >= common.GetMajorityCount(len(responses)) {
					return true
				}
			}
		case <-electionTimer.C:
			return false
		}
	}
}

func (p *PreCandidate) HandleAppendEntries(request protocol.AppendEntriesRequest) (*raft.ChangeStateEvent, protocol.AppendEntriesResponse, error) {
	return nil, protocol.AppendEntriesResponse{IsSucceeded: true}, nil
}

func (p *PreCandidate) HandleVote(request protocol.VoteRequest) (*raft.ChangeStateEvent, protocol.VoteResponse, error) {
	if request.Term < p.raft.GetCurrentTerm() {
		return nil, protocol.VoteResponse{
			Term:        p.raft.GetCurrentTerm(),
			VoteGranted: false,
		}, nil
	}

	// Revert to follower if the Candidate's term >= self term.
	if request.Term > p.raft.GetCurrentTerm() {
		nextState := &Follower{
			raft:            p.raft,
			transition:      make(chan *raft.ChangeStateEvent),
			timerResetEvent: make(chan struct{}),
			stopSignal:      make(chan struct{}),
		}
		return &raft.ChangeStateEvent{
			NextState: nextState,
			Term:      request.Term,
		}, protocol.VoteResponse{}, nil
	}

	latestLogEntry := p.raft.GetLatestLogEntry()
	isLogUpToDate := latestLogEntry.Term < request.LastLogTerm ||
		(latestLogEntry.Term == request.LastLogTerm && latestLogEntry.Id <= request.LastLogIndex)
	if request.Term == p.raft.GetCurrentTerm() {
		if isLogUpToDate && p.raft.IsAbleToVoteFor(request.CandidateID) {
			err := p.raft.SetVotedFor(request.CandidateID)
			if err != nil {
				return nil, protocol.VoteResponse{Term: p.raft.GetCurrentTerm(), VoteGranted: false}, err
			}
			return nil, protocol.VoteResponse{Term: p.raft.GetCurrentTerm(), VoteGranted: true}, nil
		}
	}

	return nil, protocol.VoteResponse{
		Term:        p.raft.GetCurrentTerm(),
		VoteGranted: false,
	}, nil
}

func (p *PreCandidate) HandlePreVote(request protocol.PreVoteRequest) (*raft.ChangeStateEvent, protocol.PreVoteResponse, error) {
	isGreaterTerm := request.HypotheticalTerm >= p.raft.GetCurrentTerm()

	latestLogEntry := p.raft.GetLatestLogEntry()
	isLogUpToDate := latestLogEntry.Term < request.LastLogTerm ||
		(latestLogEntry.Term == request.LastLogTerm && latestLogEntry.Id <= request.LastLogIndex)

	return nil, protocol.PreVoteResponse{
		Term:    p.raft.GetCurrentTerm(),
		Granted: isGreaterTerm && isLogUpToDate,
	}, nil
}

func (p *PreCandidate) getHypotheticalTerm() common.Term {
	return p.raft.GetCurrentTerm() + 1
}

func (p *PreCandidate) Stop() {
	p.stopSignal <- struct{}{}
<<<<<<< Updated upstream
=======
	p.isRunning = false
	defer close(p.stopSignal)
>>>>>>> Stashed changes
}
