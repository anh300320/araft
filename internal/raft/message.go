package raft

import "encoding/json"

type VoteRequest struct {
	Term         Term
	CandidateID  ServerID
	LastLogIndex LogIndex
	LastLogTerm  Term
}

type VoteResponse struct {
	Term        Term
	VoteGranted bool
}

type AppendEntriesRequest struct {
	MasterID     ServerID
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	LogEntry     []LogEntry
}

type AppendEntriesResponse struct {
	successful bool
}

type Event int

const (
	EventHeartBeat Event = iota
	EventAppendEntries
	EventVote
)

type EventMessage struct {
	Event Event
	body  string
}

func ParseMessage[T any](rawEvent EventMessage) (T, error) {
	var result T

	err := json.Unmarshal([]byte(rawEvent.body), &result)
	if err != nil {
		return result, err
	}

	return result, nil
}
