package states

import (
	"sync"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
	"github.com/anh300320/araft/internal/raft/transport"
	"go.uber.org/zap"
)

type PreCandidate struct {
	raft         *raft.Raft
	NextTerm     common.Term
	LastLogIndex common.LogIndex
	LastLogTerm  common.Term

	others     []transport.Transport
	transition chan raft.State
}

func (p *PreCandidate) Run() {
	go p.raft.HandleMessage()

	responses := make(chan protocol.PreVoteResponse, len(p.others))
	p.sendPreVotes(responses)
	p.HandlePreVoteResponses(responses, p.transition)
}

func (p *PreCandidate) GetTransition() chan raft.State {
	return p.transition
}

func (p *PreCandidate) sendPreVotes(responses chan protocol.PreVoteResponse) {
	var wg sync.WaitGroup
	for _, other := range p.others {
		request := protocol.PreVoteRequest{
			Term:         p.NextTerm,
			LastLogIndex: p.LastLogIndex,
			LastLogTerm:  p.LastLogTerm,
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

func (p *PreCandidate) HandlePreVoteResponses(responses chan protocol.PreVoteResponse, transitionSignal chan raft.State) {
	successCount := 0
	for preVoteResponse := range responses {
		if preVoteResponse.IsSucceeded {
			successCount += 1
			if successCount >= common.GetMajorityCount(len(p.others)) {
				candidateState := &Candidate{
					currentTerm: p.raft.GetCurrentTerm() + 1,
					transition:  make(chan raft.State),
				}
				transitionSignal <- candidateState
			}
		}
	}
}

func (p *PreCandidate) HandleHeartBeat(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	return protocol.AppendEntriesResponse{IsSucceeded: true}, nil
}

func (p *PreCandidate) HandleAppendEntries(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	return protocol.AppendEntriesResponse{IsSucceeded: true}, nil
}

func (p *PreCandidate) HandleVote(request protocol.VoteRequest) (protocol.VoteResponse, error) {
	return protocol.VoteResponse{
		Term:        p.NextTerm,
		VoteGranted: false,
	}, nil
}

func (p *PreCandidate) HandlePreVote(request protocol.PreVoteRequest) (protocol.PreVoteResponse, error) {
	return protocol.PreVoteResponse{IsSucceeded: true}, nil
}

func (p *PreCandidate) GetCurrentTerm() common.Term {
	return p.NextTerm
}
