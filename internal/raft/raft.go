package raft

import (
	"errors"

	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
	"github.com/anh300320/araft/internal/raft/transport"
	"go.uber.org/zap"
)

type Raft struct {
	serverID    common.ServerID
	currentTerm common.Term
	votedFor    common.ServerID
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
	eventChan, err := r.transport.StartListening()
	if err != nil {
		r.Logger.Fatal("failed to start listening to messages")
		panic(err) // TODO check?
	}
	defer close(eventChan)

	for {
		go r.state.Run()
		select {
		case nextState := <-r.state.GetTransition():
			oldState := r.state
			r.state = nextState
			err := oldState.Close()
			if err != nil {
				r.Logger.Error("failed to close the old state", zap.Error(err))
			}
		case event, ok := <-eventChan:
			if ok == false {
				panic("the event channel has been closed unexpectedly")
			}
			err := r.handleMessage(event)
			if err != nil {
				r.Logger.Error("failed to handle event", zap.Error(err))
			}
		}

	}
}

func (r *Raft) GetServerID() common.ServerID {
	return r.serverID
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
	if len(r.logs) == 0 {
		return common.LogEntry{ // TODO: ?
			Id:   0,
			Term: 0,
		}
	}
	return r.logs[len(r.logs)-1]
}

func (r *Raft) SetTerm(newTerm common.Term) error {
	if newTerm <= r.currentTerm {
		return errors.New("failed to assign new term, the new term should be greater than the current term")
	}
	r.currentTerm = newTerm
	return nil
}

func (r *Raft) UpgradeTerm(term common.Term) error {
	err := r.SetTerm(term) // TODO implement atomicity for these function
	if err != nil {
		return err
	}
	r.ResetVotedFor()
	return nil
}

func (r *Raft) SetVotedFor(candidateID common.ServerID) error {
	if r.votedFor != 0 && r.votedFor != candidateID {
		return errors.New("failed to assign vote, already voted")
	}
	r.votedFor = candidateID
	return nil
}

func (r *Raft) ResetVotedFor() {
	r.votedFor = 0
}

func (r *Raft) GetVotedFor() common.ServerID {
	return r.votedFor
}

func (r *Raft) handleMessage(msg protocol.EventMessage) error {
	switch msg.Event {
	case protocol.EventHeartBeat:
		appendEntriesRequest := msg.Body.(protocol.AppendEntriesRequest)
		resp, err := r.state.HandleHeartBeat(appendEntriesRequest)
		if err != nil {
			r.Logger.Error("failed to handle heartbeat message")
			return err
		}
		msg.ResponseChan <- resp

	case protocol.EventAppendEntries:
		appendEntriesRequest := msg.Body.(protocol.AppendEntriesRequest)
		resp, err := r.state.HandleAppendEntries(appendEntriesRequest)
		if err != nil {
			r.Logger.Error("failed to handle heartbeat message")
			return err
		}
		msg.ResponseChan <- resp

	case protocol.EventPreVote:
		prevVoteRequest := msg.Body.(protocol.PreVoteRequest)
		resp, err := r.state.HandlePreVote(prevVoteRequest)
		if err != nil {
			r.Logger.Error("failed tp handle prevote message")
			return err
		}
		msg.ResponseChan <- resp

	case protocol.EventVote:
		voteRequest := msg.Body.(protocol.VoteRequest)
		resp, err := r.state.HandleVote(voteRequest)
		if err != nil {
			r.Logger.Error("failed to handle vote message")
			return err
		}
		msg.ResponseChan <- resp
	}
	return nil
}
