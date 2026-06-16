package transport

import (
	"errors"

	"github.com/anh300320/araft/internal/raft/protocol"
)

type Transport interface {
	StartListening() (chan protocol.EventMessage, error)
	SendHeartBeat(other Transport, request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error)
	AppendEntries(other Transport, request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error)
	SendVote(other Transport, request protocol.VoteRequest) (protocol.VoteResponse, error)
	SendPreVote(other Transport, request protocol.PreVoteRequest) (protocol.PreVoteResponse, error)
	GetAddress() string
}

var SerializeMessageError = errors.New("failed to serialize message")
var HeartBeatMessageError = errors.New("failed to send heartbeat")
