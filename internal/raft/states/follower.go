package states

import (
	"time"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/protocol"
	"github.com/anh300320/araft/internal/raft/settings"
	"go.uber.org/zap"
)

type Follower struct {
	raft *raft.Raft

	timer     *time.Timer
	isRunning bool

	transition      chan *raft.ChangeStateEvent
	timerResetEvent chan struct{}
	stopSignal      chan struct{}
}

func NewFollower(r *raft.Raft, config settings.Config) *raft.Raft {
	followerState := &Follower{
		raft:            r,
		timer:           nil,
		isRunning:       false,
		transition:      make(chan *raft.ChangeStateEvent),
		timerResetEvent: make(chan struct{}),
		stopSignal:      make(chan struct{}),
	}
	r.UpdateState(followerState)
	return r
}

func (f *Follower) Run() {
	f.isRunning = true
	go f.run()
}

func (f *Follower) run() {
	defer close(f.timerResetEvent)
	defer close(f.transition)
	defer func() {
		f.isRunning = false
	}()
	f.isRunning = true
	f.raft.Logger.Info("follower running...")
	f.monitorHeartBeat()
	f.raft.Logger.Info("follower stopped...")
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
	f.resetElectionTimer()
	for {
		select {
		case <-f.stopSignal:
			return
		case <-f.timer.C:
			f.startElection()
			return
		case <-f.timerResetEvent:
			f.resetElectionTimer()
		}
	}
}

func (f *Follower) startElection() {
	nextState := &PreCandidate{
		raft:         f.raft,
		LastLogIndex: 0,
		LastLogTerm:  0,
		transition:   make(chan *raft.ChangeStateEvent),
	}
	f.transition <- &raft.ChangeStateEvent{
		NextState: nextState,
		Term:      f.raft.GetCurrentTerm(),
	}
}

func (f *Follower) HandleAppendEntries(request protocol.AppendEntriesRequest) (*raft.ChangeStateEvent, protocol.AppendEntriesResponse, error) {
	f.timerResetEvent <- struct{}{}
	if request.Term > f.raft.GetCurrentTerm() {
		return &raft.ChangeStateEvent{
			NextState: nil,
			Term:      request.Term,
		}, protocol.AppendEntriesResponse{}, nil
	}

	return nil, protocol.AppendEntriesResponse{IsSucceeded: true}, nil
}

func (f *Follower) HandleVote(request protocol.VoteRequest) (*raft.ChangeStateEvent, protocol.VoteResponse, error) {
	if request.Term < f.raft.GetCurrentTerm() {
		return nil, protocol.VoteResponse{
			Term:        f.raft.GetCurrentTerm(),
			VoteGranted: false,
		}, nil
	}

	if request.Term > f.raft.GetCurrentTerm() {
		return &raft.ChangeStateEvent{
			Term: request.Term,
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
		(latestLogEntry.Term == request.LastLogTerm && latestLogEntry.Id <= request.LastLogIndex)

	if isLogUpToDate {
		err := f.raft.SetVotedFor(request.CandidateID)
		if err != nil {
			return nil, protocol.VoteResponse{
				Term:        f.raft.GetCurrentTerm(),
				VoteGranted: false,
			}, err
		}
		f.resetElectionTimer()

		return nil, protocol.VoteResponse{
			Term:        f.raft.GetCurrentTerm(),
			VoteGranted: true,
		}, nil
	}

	return nil, protocol.VoteResponse{
		Term:        f.raft.GetCurrentTerm(),
		VoteGranted: false,
	}, nil
}

func (f *Follower) HandlePreVote(request protocol.PreVoteRequest) (*raft.ChangeStateEvent, protocol.PreVoteResponse, error) {

	isNewTerm := f.raft.GetCurrentTerm() < request.HypotheticalTerm

	latestLogEntry := f.raft.GetLatestLogEntry()
	isLogUpToDate := latestLogEntry.Term < request.LastLogTerm ||
		(latestLogEntry.Term == request.LastLogTerm && latestLogEntry.Id <= request.LastLogIndex)

	return nil, protocol.PreVoteResponse{
		Term:    f.raft.GetCurrentTerm(),
		Granted: isNewTerm && isLogUpToDate,
	}, nil
}

func (f *Follower) resetElectionTimer() {
	timeout := f.raft.RandomElectionTimeout()
	f.raft.Logger.Info("Resetting election timer", zap.Int("timeout_ms", int(timeout.Milliseconds())))
	f.timer.Reset(timeout)
}

func (f *Follower) Stop() {
	f.stopSignal <- struct{}{}
	f.isRunning = false
	defer close(f.stopSignal)
}
