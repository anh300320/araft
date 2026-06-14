package raft

type LogEntry struct {
	Id   LogIndex
	Term Term
	data LogData
}
