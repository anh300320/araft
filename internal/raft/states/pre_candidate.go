package states

import (
	"sync"
	"time"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
	"github.com/anh300320/araft/internal/raft/transport"
	"go.uber.org/zap"
)

type PreCandidate struct {
	raft         *raft.Raft
	LastLogIndex common.LogIndex
	LastLogTerm  common.Term

	others     []transport.Transport
	transition chan raft.State
}

func (p *PreCandidate) Start() error {
	return nil
}

func (p *PreCandidate) Run() {
	responses := make(chan protocol.PreVoteResponse, len(p.others))
	defer close(responses)
	p.sendPreVoteRequests(responses)
	p.handlePreVoteResponses(responses, p.transition)
}

func (p *PreCandidate) GetTransition() chan raft.State {
	return p.transition
}

func (p *PreCandidate) sendPreVoteRequests(responses chan protocol.PreVoteResponse) {
	var wg sync.WaitGroup
	for _, other := range p.others {
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

func (p *PreCandidate) handlePreVoteResponses(responses chan protocol.PreVoteResponse, transitionSignal chan raft.State) {
	successCount := 0
	for preVoteResponse := range responses {
		if preVoteResponse.Granted {
			successCount += 1
			if successCount >= common.GetMajorityCount(len(p.others)) {
				candidateState := &Candidate{
					raft:       p.raft,
					transition: make(chan raft.State),
				}
				transitionSignal <- candidateState
				break
			}
		}
	}
}

func (p *PreCandidate) HandleHeartBeat(request protocol.AppendEntriesRequest) (raft.State, protocol.AppendEntriesResponse, error) {
	return nil, protocol.AppendEntriesResponse{IsSucceeded: true}, nil
}

func (p *PreCandidate) HandleAppendEntries(request protocol.AppendEntriesRequest) (raft.State, protocol.AppendEntriesResponse, error) {
	return nil, protocol.AppendEntriesResponse{IsSucceeded: true}, nil
}

func (p *PreCandidate) HandleVote(request protocol.VoteRequest) (raft.State, protocol.VoteResponse, error) {
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
			lastHeartBeatAt: time.Now(),
			monitorInterval: 0,
			electionTimeout: 0,
			isRunning:       false,
			transition:      make(chan raft.State),
		}
		err := p.raft.UpgradeTerm(request.Term)
		if err != nil {
			p.raft.Logger.Error("failed to upgrade term", zap.Error(err))
			return nextState, protocol.VoteResponse{}, err
		}
		return nextState, protocol.VoteResponse{}, nil
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

func (p *PreCandidate) HandlePreVote(request protocol.PreVoteRequest) (raft.State, protocol.PreVoteResponse, error) {
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

func (p *PreCandidate) Close() error {
	close(p.transition)
	return nil
}
