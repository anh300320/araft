package raft

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/persistent"
	"github.com/anh300320/araft/internal/raft/protocol"
	"github.com/anh300320/araft/internal/raft/settings"
	"github.com/anh300320/araft/internal/raft/transport"
	"go.uber.org/zap"
)

type Raft struct {
	serverID common.ServerID

	currentTerm common.Term
	votedFor    common.ServerID
	logs        []common.LogEntry

	// Volatile states
	commitIndex common.LogIndex
	lastApplied common.LogIndex

	state  State
	Logger *zap.Logger

	transport transport.Transport
	peers     []Peer
	// Volatile states for leaders
	//nextIndex  []LogIndex
	//matchIndex []LogIndex

	persistent         persistent.NodeStatePersistent
	logEntryPersistent persistent.LogEntryPersistent

	baseElectionTimoutMs int
}

func NewRaftNode(logger *zap.Logger, config settings.Config) *Raft {
	nodeTransport := transport.NewHttpTransport(logger, config.Hostname, config.Port)
	nodePersistent := persistent.NewSimpleFilePersistent(logger, config.GetDataPath())
	nodeState, err := nodePersistent.GetState()
	if err != nil {
		logger.Error(
			"failed to load node state",
			zap.Int("node_id", config.NodeID),
			zap.Error(err),
		)
		panic(err)
	}
	return &Raft{
		serverID:    common.ServerID(config.NodeID),
		currentTerm: nodeState.Term,
		votedFor:    nodeState.VotedFor,
		logs:        []common.LogEntry{},
		commitIndex: 0,
		lastApplied: 0,
		state:       nil,
		Logger: logger.With(
			zap.Int("node_id", config.NodeID),
		),
		transport:            nodeTransport,
		peers:                buildPeers(logger, config),
		persistent:           nodePersistent,
		baseElectionTimoutMs: config.ElectionTimeoutBaseMs,
	}
}

func (r *Raft) RandomElectionTimeout() time.Duration {
	return time.Duration(r.baseElectionTimoutMs+rand.Intn(150)) * time.Millisecond
}

func buildPeers(logger *zap.Logger, config settings.Config) []Peer {
	peers := make([]Peer, 0, len(config.Peers))
	for _, peerConfig := range config.Peers {
		if peerConfig.ServerID == config.NodeID {
			continue
		}
		peers = append(peers, Peer{
			ServerID: common.ServerID(peerConfig.ServerID),
			transport: transport.NewHttpTransport(
				logger, // TODO: use a different logger here
				peerConfig.Hostname,
				peerConfig.Port,
			),
		})
	}
	return peers
}

func (r *Raft) Run() {
	eventChan, err := r.transport.StartListening()
	if err != nil {
		r.Logger.Fatal("failed to start listening to messages")
		panic(err) // TODO check?
	}
	defer close(eventChan)

	for {
		if !r.state.IsRunning() {
			r.state.Run()
		}

		select {
		case nextState := <-r.state.GetTransition():
			r.HandleChangeState(nextState)
		case event, ok := <-eventChan:
			if !ok {
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

func (r *Raft) GetPeers() []Peer {
	return r.peers
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
			Index: 0,
			Term:  0,
		}
	}
	return r.logs[len(r.logs)-1]
}

func (r *Raft) SetVotedFor(candidateID common.ServerID) error {
	if !r.IsAbleToVoteFor(candidateID) {
		return fmt.Errorf("failed to assign vote, already voted for %d", r.votedFor)
	}
	r.votedFor = candidateID
	return r.flushState()
}

func (r *Raft) IsAbleToVoteFor(candidateID common.ServerID) bool {
	return r.votedFor == 0 || r.votedFor == candidateID
}

func (r *Raft) UpdateState(nextState State) error {
	t := reflect.TypeOf(nextState).Elem()
	r.Logger.Info("updating states to", zap.String("next_state:", t.Name()))
	if r.state != nil {
		r.state.Stop()
	}
	r.state = nextState
	return nil
}

func (r *Raft) HandleChangeState(c *ChangeStateEvent) {
	err := r.UpdateState(c.NextState)
	if err != nil {
		panic(err)
	}
	err = func() error {
		err := r.setTerm(c.Term)
		if err != nil {
			return err
		}
		r.votedFor = c.VotedFor
		return r.flushState()
	}()
	if err != nil {
		r.Logger.Error("failed to update node state", zap.Error(err))
		panic(err)
	}
}

func (r *Raft) setTerm(newTerm common.Term) error {
	if newTerm <= r.currentTerm {
		return fmt.Errorf(
			"failed to assign new term, the new term should be greater than the current term. currentTerm: %d, newTerm: %d",
			r.currentTerm, newTerm,
		)
	}
	r.currentTerm = newTerm
	return nil
}

func (r *Raft) flushState() error {
	persistentState := persistent.NodeState{
		Term:     r.currentTerm,
		VotedFor: r.votedFor,
	}
	return r.persistent.UpdateState(persistentState)
}

func (r *Raft) AppendLogEntry(logData common.LogData) (common.LogEntry, error) {
	logEntry := common.LogEntry{
		Index: r.lastApplied,
		Term:  r.currentTerm,
		Data:  logData,
	}
	return r.logEntryPersistent.AppendEntry(logEntry)
}

func (r *Raft) handleMessage(msg protocol.EventMessage) error {
	switch msg.Event {
	case protocol.EventAppendEntries:
		appendEntriesRequest := msg.Body.(protocol.AppendEntriesRequest)
		nextState, resp, err := r.state.HandleAppendEntries(appendEntriesRequest)
		for nextState != nil {
			r.HandleChangeState(nextState)
			nextState, resp, err = r.state.HandleAppendEntries(appendEntriesRequest)
		}
		if err != nil {
			r.Logger.Error("failed to handle heartbeat message")
			return err
		}
		msg.ResponseChan <- resp

	case protocol.EventPreVote:
		prevVoteRequest := msg.Body.(protocol.PreVoteRequest)
		nextState, resp, err := r.state.HandlePreVote(prevVoteRequest)
		for nextState != nil {
			r.HandleChangeState(nextState)
			nextState, resp, err = r.state.HandlePreVote(prevVoteRequest)
		}
		if err != nil {
			r.Logger.Error("failed tp handle prevote message")
			return err
		}
		msg.ResponseChan <- resp

	case protocol.EventVote:
		voteRequest := msg.Body.(protocol.VoteRequest)
		nextState, resp, err := r.state.HandleVote(voteRequest)
		for nextState != nil {
			r.HandleChangeState(nextState)
			nextState, resp, err = r.state.HandleVote(voteRequest)
		}
		if err != nil {
			r.Logger.Error("failed to handle vote message")
			return err
		}
		msg.ResponseChan <- resp

	case protocol.EventClientAppendEntry:
		req := msg.Body.(protocol.ClientAppendEntryRequest)
		resp, err := r.state.HandleClientAppendEntry(req)
		if err != nil {
			r.Logger.Error("failed to handle client append entry") // TODO add err Info
			return err
		}
		msg.ResponseChan <- resp
	}
	return nil
}

func (r *Raft) GetLogEntriesStartAt(logIndex common.LogIndex, limit int) ([]common.LogEntry, error) {
	return r.logEntryPersistent.GetLogEntriesStartAt(logIndex, limit)
}

type Peer struct {
	serverID  common.ServerID
	transport transport.Transport
}

func (p *Peer) GetServerID() common.ServerID {
	return p.serverID
}

func (p *Peer) GetTransport() transport.Transport {
	return p.transport
}
