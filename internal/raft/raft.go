package raft

import (
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
	"github.com/anh300320/araft/internal/raft/transport"
	"go.uber.org/zap"
)

type Raft struct {
	serverID    common.ServerID
	currentTerm common.Term
	votedFor    string
	logs        []common.LogEntry

	// Volatile states
	commitIndex common.LogIndex
	lastApplied common.LogIndex

	state  State
	Logger zap.Logger

	transport transport.Transport
	others    []transport.Transport

	// Volatile states for leaders
	//nextIndex  []LogIndex
	//matchIndex []LogIndex
	//
	//followers []Raft
	//transport Transport
}

func (r *Raft) Run() {
	for {
		go r.state.Run()
		r.state = <-r.state.GetTransition()
	}
}

func (r *Raft) GetCurrentTerm() common.Term {
	return r.currentTerm
}

func (r *Raft) GetOthers() []transport.Transport {
	return r.others
}

func (r *Raft) GetCommitIndex() common.LogIndex {
	return r.commitIndex
}

func (r *Raft) GetTransport() transport.Transport {
	return r.transport
}

func (r *Raft) GetLogEntry(logIndex common.LogIndex) common.LogEntry {
	return r.logs[logIndex]
}

func (r *Raft) GetLatestLogEntry() common.LogEntry {
	return r.logs[len(r.logs)-1]
}

func (r *Raft) HandleMessage() {
	eventChan, err := r.transport.StartListening()
	if err != nil {
		r.Logger.Fatal("failed to start listening to messages")
		panic(err) // TODO check?
	}

	for msg := range eventChan {
		switch msg.Event {
		case protocol.EventHeartBeat:
			appendEntriesRequest := msg.Body.(protocol.AppendEntriesRequest)
			resp, err := r.state.HandleHeartBeat(appendEntriesRequest)
			if err != nil {
				r.Logger.Error("failed to handle heartbeat message")
			}
			msg.ResponseChan <- resp

		case protocol.EventAppendEntries:
			appendEntriesRequest := msg.Body.(protocol.AppendEntriesRequest)
			resp, err := r.state.HandleAppendEntries(appendEntriesRequest)
			if err != nil {
				r.Logger.Error("failed to handle heartbeat message")
			}
			msg.ResponseChan <- resp

		case protocol.EventPreVote:
			prevVoteRequest := msg.Body.(protocol.PreVoteRequest)
			resp, err := r.state.HandlePreVote(prevVoteRequest)
			if err != nil {
				r.Logger.Error("failed tp handle prevote message")
			}
			msg.ResponseChan <- resp

		case protocol.EventVote:
			voteRequest := msg.Body.(protocol.VoteRequest)
			resp, err := r.state.HandleVote(voteRequest)
			if err != nil {
				r.Logger.Error("failed to handle vote message")
			}
			msg.ResponseChan <- resp
		}
	}
}
