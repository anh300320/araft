package states

import (
	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
)

type Master struct {
	// Volatile states for leaders
	raft *raft.Raft

	nextIndex  []common.LogIndex
	matchIndex []common.LogIndex
}

func (m *Master) Run() {
	//TODO implement me
	panic("implement me")
}

func (m *Master) GetTransition() chan raft.State {
	//TODO implement me
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
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
