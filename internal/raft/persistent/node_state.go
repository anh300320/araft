package persistent

import "github.com/anh300320/araft/internal/raft/common"

type NodeState struct {
	Term     common.Term     `json:"term"`
	VotedFor common.ServerID `json:"voted_for"`
}

type NodeStatePersistent interface {
	UpdateState(state NodeState) error
	GetState() (*NodeState, error)
}
