package common

type LogEntry struct {
	Index LogIndex
	Term  Term
	Data  LogData
}
