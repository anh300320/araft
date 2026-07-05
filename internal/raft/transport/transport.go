package transport

import (
	"errors"

	"github.com/anh300320/araft/internal/raft/protocol"
)

type Transport interface {
	StartListening() (chan protocol.EventMessage, error)
	AppendEntries(other Transport, request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error)
	SendVote(other Transport, request protocol.VoteRequest) (protocol.VoteResponse, error)
	SendPreVote(other Transport, request protocol.PreVoteRequest) (protocol.PreVoteResponse, error)
	GetAddress() string
}

var ErrSerializeMessage = errors.New("failed to serialize message")
