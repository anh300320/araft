package states

import (
	"time"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/protocol"
	"go.uber.org/zap"
)

type Follower struct {
	raft *raft.Raft

	timer     *time.Timer
	isRunning bool

	transition      chan *raft.ChangeStateEvent
	timerResetEvent chan struct{}
	stopSignal      chan struct{}
	done            chan struct{}
}

func NewFollower(r *raft.Raft) *Follower {
	return &Follower{
		raft:            r,
		isRunning:       false,
		transition:      make(chan *raft.ChangeStateEvent),
		timerResetEvent: make(chan struct{}),
		stopSignal:      make(chan struct{}),
		done:            make(chan struct{}),
	}
}

func (f *Follower) Run() {
	f.isRunning = true
	go f.run()
}

func (f *Follower) run() {
	defer close(f.timerResetEvent)
	defer close(f.transition)
	defer close(f.done)
	defer func() {
		f.isRunning = false
	}()
	f.isRunning = true
	f.raft.Logger().Info("follower running...")
	f.monitorHeartBeat()
	f.raft.Logger().Info("follower stopped...")
}

func (f *Follower) IsRunning() bool {
	return f.isRunning
}

func (f *Follower) GetTransition() chan *raft.ChangeStateEvent {
	return f.transition
}

func (f *Follower) monitorHeartBeat() {
	f.timer = time.NewTimer(f.raft.RandomElectionTimeout())
	defer f.timer.Stop()
	for {
		select {
		case <-f.stopSignal:
			return
		case <-f.timer.C:
			f.startElection()
		case <-f.timerResetEvent:
			f.resetElectionTimer()
		}
	}
}

func (f *Follower) startElection() {
	nextState := &PreCandidate{
		raft:       f.raft,
		transition: make(chan *raft.ChangeStateEvent),
		stopSignal: make(chan struct{}),
	}
	f.transition <- &raft.ChangeStateEvent{
		NextState: nextState,
		Term:      f.raft.GetCurrentTerm(),
	}
}

func (f *Follower) HandleAppendEntries(request protocol.AppendEntriesRequest) (*raft.ChangeStateEvent, protocol.AppendEntriesResponse, error) {
	f.timerResetEvent <- struct{}{}
	lastLogEntry := f.raft.GetLatestLogEntry()
	response := protocol.AppendEntriesResponse{
		IsSucceeded:  false,
		Term:         f.raft.GetCurrentTerm(),
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
	if f.raft.GetCurrentTerm() < request.Term {
		return &raft.ChangeStateEvent{
			NextState: NewFollower(f.raft),
			Term:      request.Term,
		}, response, nil
	}

	if f.raft.GetCurrentTerm() > request.Term {
		return nil, response, nil
	}

	response.IsSucceeded = true
	return nil, response, nil
}

func (f *Follower) HandleVote(request protocol.VoteRequest) (*raft.ChangeStateEvent, protocol.VoteResponse, error) {
	f.raft.Logger().Info(
		"handling vote request", zap.Int32("request_term", request.Term),
		zap.Int("candidate_id", int(request.CandidateID)),
		zap.Int("current_term", int(f.raft.GetCurrentTerm())),
	)
	if request.Term < f.raft.GetCurrentTerm() {
		return nil, protocol.VoteResponse{
			Term:        f.raft.GetCurrentTerm(),
			VoteGranted: false,
		}, nil
	}

	if request.Term > f.raft.GetCurrentTerm() {
		return &raft.ChangeStateEvent{
			Term:      request.Term,
			NextState: NewFollower(f.raft),
		}, protocol.VoteResponse{}, nil
	}

	if !f.raft.IsAbleToVoteFor(request.CandidateID) {
		return nil, protocol.VoteResponse{
			Term:        f.raft.GetCurrentTerm(),
			VoteGranted: false,
		}, nil
	}

	latestLogEntry := f.raft.GetLatestLogEntry()
	isLogUpToDate := latestLogEntry.Term < request.LastLogTerm ||
		(latestLogEntry.Term == request.LastLogTerm && latestLogEntry.Index <= request.LastLogIndex)

	if isLogUpToDate {
		err := f.raft.SetVotedFor(request.CandidateID)
		if err != nil {
			f.raft.Logger().Error("failed to set voted for", zap.Int("candidate_id", int(request.CandidateID)), zap.Error(err))
			return nil, protocol.VoteResponse{
				Term:        f.raft.GetCurrentTerm(),
				VoteGranted: false,
			}, err
		}
		f.timerResetEvent <- struct{}{}

		f.raft.Logger().Info("grant vote for candidate", zap.Int("candidate_id", int(request.CandidateID)), zap.Error(err))
		return nil, protocol.VoteResponse{
			Term:        f.raft.GetCurrentTerm(),
			VoteGranted: true,
		}, nil
	}

	f.raft.Logger().Info("refuse to grant vote for candidate", zap.Int("candidate_id", int(request.CandidateID)))
	return nil, protocol.VoteResponse{
		Term:        f.raft.GetCurrentTerm(),
		VoteGranted: false,
	}, nil
}

func (f *Follower) HandlePreVote(request protocol.PreVoteRequest) (*raft.ChangeStateEvent, protocol.PreVoteResponse, error) {

	isNewTerm := f.raft.GetCurrentTerm() < request.HypotheticalTerm

	latestLogEntry := f.raft.GetLatestLogEntry()
	isLogUpToDate := latestLogEntry.Term < request.LastLogTerm ||
		(latestLogEntry.Term == request.LastLogTerm && latestLogEntry.Index <= request.LastLogIndex)

	return nil, protocol.PreVoteResponse{
		Term:    f.raft.GetCurrentTerm(),
		Granted: isNewTerm && isLogUpToDate,
	}, nil
}

func (f *Follower) HandleClientAppendEntry(request protocol.ClientAppendEntryRequest) (protocol.ClientAppendEntryResponse, error) {
	return protocol.ClientAppendEntryResponse{}, nil
}

func (f *Follower) resetElectionTimer() {
	timeout := f.raft.RandomElectionTimeout()
	f.raft.Logger().Info("Resetting election timer", zap.Int("timeout_ms", int(timeout.Milliseconds())))
	f.timer.Reset(timeout)
}

func (f *Follower) Stop() {
	close(f.stopSignal)
	<-f.done
	f.isRunning = false
}
