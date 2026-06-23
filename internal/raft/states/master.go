package states

import (
	"time"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
	"go.uber.org/zap"
)

type Master struct {
	// Volatile states for leaders
	raft *raft.Raft

	nextIndex  []common.LogIndex
	matchIndex []common.LogIndex

	transition chan raft.State
}

func (m *Master) Run() {
	//TODO implement me
	panic("implement me")
}

func (m *Master) GetTransition() chan raft.State {
	return m.transition
}

func (m *Master) HandleHeartBeat(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Master) HandleAppendEntries(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Master) HandleVote(request protocol.VoteRequest) (protocol.VoteResponse, error) {
	if request.Term <= m.raft.GetCurrentTerm() {
		return protocol.VoteResponse{
			Term:        m.raft.GetCurrentTerm(),
			VoteGranted: false,
		}, nil
	}

	// Reaching here means the Candidate's term is greater than self term.
	defer func() {
		nextState := &Follower{
			raft:            m.raft,
			lastHeartBeatAt: time.Now(),
			monitorInterval: 0,
			electionTimeout: 0,
			isRunning:       false,
			transition:      make(chan raft.State),
		}
		m.transition <- nextState
	}()

	err := m.raft.UpgradeTerm(request.Term)
	if err != nil {
		m.raft.Logger.Error("failed to upgrade term", zap.Error(err))
		return protocol.VoteResponse{}, err
	}
	latestLogEntry := m.raft.GetLatestLogEntry()
	isLogUpToDate := latestLogEntry.Term < request.LastLogTerm ||
		(latestLogEntry.Term == request.LastLogTerm && latestLogEntry.Id <= request.LastLogIndex)

	if isLogUpToDate {
		err = m.raft.SetVotedFor(request.CandidateID)
		if err != nil {
			m.raft.Logger.Error("failed to set vote", zap.Error(err))
			return protocol.VoteResponse{Term: m.raft.GetCurrentTerm(), VoteGranted: false}, err
		}
		return protocol.VoteResponse{Term: m.raft.GetCurrentTerm(), VoteGranted: true}, err
	}

	return protocol.VoteResponse{Term: m.raft.GetCurrentTerm(), VoteGranted: false}, nil
}

func (m *Master) HandlePreVote(request protocol.PreVoteRequest) (protocol.PreVoteResponse, error) {
	isGreaterTerm := request.HypotheticalTerm > m.raft.GetCurrentTerm()

	latestLogEntry := m.raft.GetLatestLogEntry()
	isLogUpToDate := latestLogEntry.Term < request.LastLogTerm ||
		(latestLogEntry.Term == request.LastLogTerm && latestLogEntry.Id <= request.LastLogIndex)

	return protocol.PreVoteResponse{
		Term:    m.raft.GetCurrentTerm(),
		Granted: isGreaterTerm && isLogUpToDate,
	}, nil
}

func (m *Master) Close() error {
	return nil
}
