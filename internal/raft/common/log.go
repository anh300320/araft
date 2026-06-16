package common

type LogEntry struct {
	Id   LogIndex
	Term Term
	Data LogData
}
