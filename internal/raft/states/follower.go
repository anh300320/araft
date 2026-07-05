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

	transition      chan raft.State
	timerResetEvent chan struct{}
}

func NewFollower(r *raft.Raft, config settings.Config) *raft.Raft {
	followerState := &Follower{
		raft:            r,
		timer:           nil,
		isRunning:       false,
		transition:      make(chan raft.State),
		timerResetEvent: make(chan struct{}),
	}
	r.ChangeState(followerState)
	return r
}

func (f *Follower) Start() error {
	f.isRunning = true
	return nil
}

func (f *Follower) Run() {
	f.raft.Logger.Info("Running...")
	go f.monitorHeartBeat()
}

func (f *Follower) GetTransition() chan raft.State {
	return f.transition
}

func (f *Follower) monitorHeartBeat() {
	f.timer = time.NewTimer(f.raft.RandomElectionTimeout())
	f.resetElectionTimer()
	for f.isRunning {
		select {
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
		raft:         &raft.Raft{},
		LastLogIndex: 0,
		LastLogTerm:  0,
		transition:   make(chan raft.State),
	}
	f.transition <- nextState
}

func (f *Follower) HandleAppendEntries(request protocol.AppendEntriesRequest) (raft.State, protocol.AppendEntriesResponse, error) {
	f.timerResetEvent <- struct{}{}
	if request.Term > f.raft.GetCurrentTerm() {
		err := f.raft.UpgradeTerm(request.Term)
		if err != nil {
			return nil, protocol.AppendEntriesResponse{}, err
		}
	}

	return nil, protocol.AppendEntriesResponse{IsSucceeded: true}, nil
}

func (f *Follower) HandleVote(request protocol.VoteRequest) (raft.State, protocol.VoteResponse, error) {
	if request.Term < f.raft.GetCurrentTerm() {
		return nil, protocol.VoteResponse{
			Term:        f.raft.GetCurrentTerm(),
			VoteGranted: false,
		}, nil
	}

	if request.Term > f.raft.GetCurrentTerm() {
		err := f.raft.UpgradeTerm(request.Term)
		if err != nil {
			f.raft.Logger.Error("failed to assign new term", zap.Error(err))
			return nil, protocol.VoteResponse{
				Term:        f.raft.GetCurrentTerm(),
				VoteGranted: false,
			}, err
		}
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

func (f *Follower) HandlePreVote(request protocol.PreVoteRequest) (raft.State, protocol.PreVoteResponse, error) {

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

func (f *Follower) Close() error {
	close(f.transition)
	f.isRunning = false
	close(f.timerResetEvent)
	f.timer.Stop()
	return nil
}
