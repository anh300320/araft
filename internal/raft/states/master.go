package states

import (
	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
	"github.com/anh300320/araft/internal/raft/transport"
)

type Master struct {
	// Volatile states for leaders
	nextIndex  []common.LogIndex
	matchIndex []common.LogIndex

	transport transport.Transport
	followers []Follower
}

func (m Master) Run() {
	//TODO implement me
	panic("implement me")
}

func (m Master) GetTransition() chan raft.State {
	//TODO implement me
	panic("implement me")
}

func (m Master) HandleHeartBeat(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m Master) HandleAppendEntries(request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m Master) HandleVote(request protocol.VoteRequest) (protocol.VoteResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m Master) HandlePreVote(request protocol.PreVoteRequest) (protocol.PreVoteResponse, error) {
	//TODO implement me
	panic("implement me")
}
