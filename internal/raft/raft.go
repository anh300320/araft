package raft

type Term = int32
type LogIndex = int64
type ServerID = int32
type LogData = string

type Raft struct {
	serverID    ServerID
	currentTerm Term
	votedFor    string
	logs        []LogEntry

	// Volatile states
	commitId    LogIndex
	lastApplied LogIndex

	// Volatile states for leaders
	//nextIndex  []LogIndex
	//matchIndex []LogIndex
	//
	//followers []Raft
	//transport Transport
}
