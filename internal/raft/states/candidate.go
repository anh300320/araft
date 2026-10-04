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
	raft      *raft.Raft
	isRunning bool

	electionTimer *time.Timer

	transition chan *raft.ChangeStateEvent
	stopSignal chan struct{}
}

func (c *Candidate) Run() {
	c.isRunning = true
	go c.run()
}

func (c *Candidate) run() {
	// FIXME: split brain issue, there are cases where 2 master exist at the same time
	defer close(c.transition)
	c.electionTimer = time.NewTimer(c.raft.RandomElectionTimeout())

	responses := c.sendVoteRequests()
	var nextState raft.State
	nextTerm := c.raft.GetCurrentTerm()
	if c.promoteToMaster(responses) {
		nextState = &Master{
			raft:       c.raft,
			transition: make(chan *raft.ChangeStateEvent),
		}
	} else {
		nextState = &Candidate{
			raft:       c.raft,
			transition: make(chan *raft.ChangeStateEvent),
		}
		nextTerm += 1
	}
	c.transition <- &raft.ChangeStateEvent{
		NextState: nextState,
		Term:      nextTerm,
		VotedFor:  c.raft.GetServerID(),
	}
	<-c.stopSignal
}

func (c *Candidate) IsRunning() bool {
	return c.isRunning
}

func (c *Candidate) sendVoteRequests() chan protocol.VoteResponse {
	lastLogEntry := c.raft.GetLatestLogEntry()
	req := protocol.VoteRequest{
		CandidateID:  c.raft.GetServerID(),
		Term:         c.raft.GetCurrentTerm(),
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}

	c.raft.Logger().Info(
		"asking for vote",
		zap.Int("Last_log_index", int(req.LastLogIndex)),
		zap.Int("last_log_term", int(req.LastLogTerm)),
	)
	peers := c.raft.GetPeers()
	responses := make(chan protocol.VoteResponse, len(peers))
	var wg sync.WaitGroup
	for _, peer := range peers {
		t := c.raft.GetTransport()
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := t.SendVote(peer.GetTransport(), req)
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
	grantedCount := 1
	receivedCount := 0
	for {
		select {
		case resp, ok := <-responses:
			receivedCount += 1
			if receivedCount >= len(responses) && !ok {
				c.raft.Logger().Info("responses channel for vote requests has been closed")
				return false
			}
			if resp.VoteGranted {
				grantedCount += 1
				majorityCnt := common.GetMajorityCount(len(responses) + 1)
				c.raft.Logger().Info("checking promote condition", zap.Int("granted_count", grantedCount), zap.Int("majority_count", majorityCnt))
				if grantedCount >= majorityCnt {
					return true
				}
			}
		case <-c.stopSignal:
			c.raft.Logger().Info("the candidate has received stop signal")
			return false
		case <-c.electionTimer.C:
			c.raft.Logger().Info("candidate election timer timeout")
			return false
		}
	}
}

func (c *Candidate) HandleAppendEntries(request protocol.AppendEntriesRequest) (*raft.ChangeStateEvent, protocol.AppendEntriesResponse, error) {
	return nil, protocol.AppendEntriesResponse{IsSucceeded: false}, nil
}

func (c *Candidate) HandleVote(request protocol.VoteRequest) (*raft.ChangeStateEvent, protocol.VoteResponse, error) {
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
			transition:      make(chan *raft.ChangeStateEvent),
			timerResetEvent: make(chan struct{}),
		}
		return &raft.ChangeStateEvent{
			NextState: nextState,
			Term:      request.Term,
		}, protocol.VoteResponse{}, nil
	}

	return nil, protocol.VoteResponse{
		Term:        c.raft.GetCurrentTerm(),
		VoteGranted: false,
	}, nil
}

func (c *Candidate) HandlePreVote(request protocol.PreVoteRequest) (*raft.ChangeStateEvent, protocol.PreVoteResponse, error) {
	isNewTerm := c.raft.GetCurrentTerm() < request.HypotheticalTerm

	latestLogEntry := c.raft.GetLatestLogEntry()
	isLogUpToDate := latestLogEntry.Term < request.LastLogTerm ||
		(latestLogEntry.Term == request.LastLogTerm && latestLogEntry.Index <= request.LastLogIndex)

	return nil, protocol.PreVoteResponse{
		Term:    c.raft.GetCurrentTerm(),
		Granted: isNewTerm && isLogUpToDate,
	}, nil
}

func (c *Candidate) HandleClientAppendEntry(request protocol.ClientAppendEntryRequest) (protocol.ClientAppendEntryResponse, error) {
	return protocol.ClientAppendEntryResponse{}, nil
}

func (c *Candidate) GetTransition() chan *raft.ChangeStateEvent {
	return c.transition
}

func (c *Candidate) Stop() {
	c.stopSignal <- struct{}{}
	c.isRunning = false
	defer close(c.stopSignal)
}
