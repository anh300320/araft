package states

import (
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/transport"
)

type Master struct {
	// Volatile states for leaders
	nextIndex  []common.LogIndex
	matchIndex []common.LogIndex

	transport transport.Transport
	followers []Follower
}
