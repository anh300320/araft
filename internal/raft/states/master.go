package states

import (
	"sync"
	"time"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
	"go.uber.org/zap"
)

type Master struct {
	// Volatile states for leaders
	raft *raft.Raft

	heartBeatTimer *time.Timer
	isRunning      bool

	nextIndex  []common.LogIndex
	matchIndex []common.LogIndex

	transition chan *raft.ChangeStateEvent
	stopSignal chan struct{}
}

func (m *Master) Run() {
	defer close(m.transition)
<<<<<<< Updated upstream
	defer close(m.stopSignal)
	defer func() {
		m.isRunning = false
	}()
	m.isRunning = true
=======
>>>>>>> Stashed changes
	m.maintainHeartBeat()
}

func (m *Master) IsRunning() bool {
	return m.isRunning
}

func (m *Master) GetTransition() chan *raft.ChangeStateEvent {
	return m.transition
}

func (m *Master) HandleAppendEntries(request protocol.AppendEntriesRequest) (*raft.ChangeStateEvent, protocol.AppendEntriesResponse, error) {
	if request.Term > m.raft.GetCurrentTerm() {
		nextState := &Follower{
			raft:            m.raft,
			isRunning:       false,
			transition:      make(chan *raft.ChangeStateEvent),
			timerResetEvent: make(chan struct{}),
		}
		return &raft.ChangeStateEvent{
			NextState: nextState,
			Term:      request.Term,
		}, protocol.AppendEntriesResponse{}, nil
	}

	if request.Term == m.raft.GetCurrentTerm() {
		m.raft.Logger.Warn("detected another leader with the same term", zap.Int64("peer_node_id", int64(int(request.MasterID))))
	}

	return nil, protocol.AppendEntriesResponse{
		Term:        m.raft.GetCurrentTerm(),
		IsSucceeded: false,
	}, nil
}

func (m *Master) HandleVote(request protocol.VoteRequest) (*raft.ChangeStateEvent, protocol.VoteResponse, error) {
	if request.Term <= m.raft.GetCurrentTerm() {
		return nil, protocol.VoteResponse{
			Term:        m.raft.GetCurrentTerm(),
			VoteGranted: false,
		}, nil
	}

	// Reaching here means the Candidate's term is greater than self term.
	nextState := &Follower{
		raft:            m.raft,
		isRunning:       false,
		transition:      make(chan *raft.ChangeStateEvent),
		timerResetEvent: make(chan struct{}),
	}
	return &raft.ChangeStateEvent{
		NextState: nextState,
		Term:      request.Term,
	}, protocol.VoteResponse{}, nil
}

func (m *Master) HandlePreVote(request protocol.PreVoteRequest) (*raft.ChangeStateEvent, protocol.PreVoteResponse, error) {
	return nil, protocol.PreVoteResponse{}, nil
}

func (m *Master) maintainHeartBeat() {
	heartBeatInterval := 100 * time.Millisecond // TODO put this interval into config
	m.heartBeatTimer = time.NewTimer(heartBeatInterval)
	defer m.heartBeatTimer.Stop()

	for {
		responsesCh := m.broadcastHeartBeat()
		isTimeout := false
		for !isTimeout {
			select {
			case resp := <-responsesCh:
				m.handleHeartBeatResponse(resp)
			case <-m.heartBeatTimer.C:
				m.heartBeatTimer.Reset(heartBeatInterval)
				isTimeout = true
			case <-m.stopSignal:
				return
			}
		}
	}
}

func (m *Master) handleHeartBeatResponse(response protocol.AppendEntriesResponse) {
	if response.Term > m.raft.GetCurrentTerm() {
		nextState := &Follower{
			raft:            m.raft,
			transition:      make(chan *raft.ChangeStateEvent),
			timerResetEvent: make(chan struct{}),
			stopSignal:      make(chan struct{}),
		}
		m.transition <- &raft.ChangeStateEvent{
			NextState: nextState,
			Term:      m.raft.GetCurrentTerm(),
		}
	}
	// TODO add more logic
}

func (m *Master) broadcastHeartBeat() chan protocol.AppendEntriesResponse {
	peers := m.raft.GetOthers()
	t := m.raft.GetTransport()
	req := protocol.AppendEntriesRequest{
		Term:              m.raft.GetCurrentTerm(),
		MasterID:          m.raft.GetServerID(),
		PrevLogIndex:      0, // TODO
		PrevLogTerm:       0, // TODO
		LeaderCommitIndex: m.raft.GetCommitIndex(),
		LogEntry:          make([]common.LogEntry, 0),
	}
	responsesCh := make(chan protocol.AppendEntriesResponse, len(peers))
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := t.SendAppendEntries(peer, req)
			if err != nil {
				m.raft.Logger.Error("failed to send heartbeat", zap.String("address", peer.GetAddress()), zap.Error(err))
			}
			responsesCh <- resp
		}()
	}

	go func() {
		wg.Wait()
		close(responsesCh)
	}()
	return responsesCh
}

func (m *Master) Stop() {
	m.stopSignal <- struct{}{}
<<<<<<< Updated upstream
=======
	m.isRunning = false
	defer close(m.stopSignal)
>>>>>>> Stashed changes
}
