package states

import (
	"time"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/protocol"
	"go.uber.org/zap"
)

type Follower struct {
	raft *raft.Raft

	lastHeartBeatAt time.Time
	monitorInterval time.Duration
	electionTimeout time.Duration
	isRunning       bool

	transition chan raft.State
}

func (f *Follower) Run() {
	f.isRunning = true
	go f.monitorHeartBeat()
}

func (f *Follower) GetTransition() chan raft.State {
	return f.transition
}

func (f *Follower) monitorHeartBeat() {
	for f.isRunning {
		if time.Now().Sub(f.lastHeartBeatAt) > f.electionTimeout {
			prevCandidateState := &PreCandidate{
				LastLogIndex: 0,
				LastLogTerm:  0,
				others:       f.raft.GetOthers(),
				transition:   make(chan raft.State),
			}
			f.transition <- prevCandidateState
			defer close(f.transition)
		}
		time.Sleep(f.monitorInterval)
	}
}

func (f *Follower) HandleHeartBeat(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	f.lastHeartBeatAt = time.Now()
	return protocol.AppendEntriesResponse{IsSucceeded: true}, nil
}

func (f *Follower) HandleAppendEntries(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	return protocol.AppendEntriesResponse{IsSucceeded: true}, nil
}

func (f *Follower) HandleVote(request protocol.VoteRequest) (protocol.VoteResponse, error) {
	if request.Term < f.raft.GetCurrentTerm() {
		return protocol.VoteResponse{
			Term:        f.raft.GetCurrentTerm(),
			VoteGranted: false,
		}, nil
	}

	if request.Term > f.raft.GetCurrentTerm() {
		err := f.raft.UpgradeTerm(request.Term)
		if err != nil {
			f.raft.Logger.Error("failed to assign new term", zap.Error(err))
			return protocol.VoteResponse{
				Term:        f.raft.GetCurrentTerm(),
				VoteGranted: false,
			}, err
		}
	}

	if f.raft.GetVotedFor() != 0 && f.raft.GetVotedFor() != request.CandidateID {
		return protocol.VoteResponse{
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
			return protocol.VoteResponse{
				Term:        f.raft.GetCurrentTerm(),
				VoteGranted: false,
			}, err
		}
		f.resetElectionTimer()

		return protocol.VoteResponse{
			Term:        f.raft.GetCurrentTerm(),
			VoteGranted: true,
		}, nil
	}

	return protocol.VoteResponse{
		Term:        f.raft.GetCurrentTerm(),
		VoteGranted: false,
	}, nil
}

func (f *Follower) HandlePreVote(request protocol.PreVoteRequest) (protocol.PreVoteResponse, error) {

	isNewTerm := f.raft.GetCurrentTerm() < request.HypotheticalTerm

	latestLogEntry := f.raft.GetLatestLogEntry()
	isLogUpToDate := latestLogEntry.Term < request.LastLogTerm ||
		(latestLogEntry.Term == request.LastLogTerm && latestLogEntry.Id <= request.LastLogIndex)

	isTimeOut := time.Now().Sub(f.lastHeartBeatAt) > f.electionTimeout

	return protocol.PreVoteResponse{
		Term:    f.raft.GetCurrentTerm(),
		Granted: isNewTerm && isLogUpToDate && isTimeOut,
	}, nil
}

func (f *Follower) resetElectionTimer() {
	f.lastHeartBeatAt = time.Now()
}

func (f *Follower) Close() error {
	close(f.transition)
	f.isRunning = false
	return nil
}
