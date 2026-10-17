package states

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
	"github.com/anh300320/araft/internal/raft/transport"
	"go.uber.org/zap"
)

var ErrBroadcastTimeout = errors.New("timeout while broadcasting")

type Master struct {
	// Volatile states for leaders
	raft *raft.Raft

	heartBeatTimer *time.Timer
	isRunning      bool

	transition chan *raft.ChangeStateEvent
	stopSignal chan struct{}
	done       chan struct{}

	followers map[common.ServerID]FollowerInfo
}

func NewMaster(r *raft.Raft) *Master {
	followers := map[common.ServerID]FollowerInfo{}
	for _, peer := range r.GetPeers() {
		followers[peer.GetServerID()] = FollowerInfo{
			serverID: peer.GetServerID(),
			synced:   false,
		}
	}
	return &Master{
		raft:       r,
		isRunning:  false,
		transition: make(chan *raft.ChangeStateEvent),
		stopSignal: make(chan struct{}),
		done:       make(chan struct{}),
		followers:  followers,
	}
}

func (m *Master) Run() {
	m.isRunning = true
	go m.run()
}

func (m *Master) run() {
	defer close(m.transition)
	defer close(m.done)
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
		nextState := NewFollower(m.raft)
		return &raft.ChangeStateEvent{
			NextState: nextState,
			Term:      request.Term,
		}, protocol.AppendEntriesResponse{}, nil
	}

	if request.Term == m.raft.GetCurrentTerm() {
		m.raft.Logger().Fatal("detected another leader with the same term", zap.Int64("peer_node_id", int64(int(request.MasterID))))
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
	nextState := NewFollower(m.raft)
	return &raft.ChangeStateEvent{
		NextState: nextState,
		Term:      request.Term,
	}, protocol.VoteResponse{}, nil
}

func (m *Master) HandlePreVote(request protocol.PreVoteRequest) (*raft.ChangeStateEvent, protocol.PreVoteResponse, error) {
	return nil, protocol.PreVoteResponse{}, nil
}

func (m *Master) maintainHeartBeat() {
	heartBeatInterval := m.raft.RandomElectionTimeout() / 3
	m.heartBeatTimer = time.NewTimer(0) // Set to 0 so the heart beat will be fired immediately
	defer m.heartBeatTimer.Stop()

	broadcastResultChan := make(chan broadcastResult)

	for {
		select {
		case <-m.heartBeatTimer.C:
			m.heartBeatTimer.Reset(heartBeatInterval)
			broadcastResultChan = m.broadcastHeartBeat()
		case r := <-broadcastResultChan:
			if r.err != nil {
				continue
			}
			m.handleHeartBeatResponse(r.resp)
		case <-m.stopSignal:
			return
		}
	}
}

func (m *Master) handleHeartBeatResponse(response protocol.AppendEntriesResponse) {
	if response.Term > m.raft.GetCurrentTerm() {
		nextState := NewFollower(m.raft)
		m.transition <- &raft.ChangeStateEvent{
			NextState: nextState,
			Term:      response.Term,
		}
	}
}

type broadcastResult struct {
	followerID common.ServerID
	resp       protocol.AppendEntriesResponse
	err        error
}

func (m *Master) broadcastHeartBeat() chan broadcastResult {
	peers := m.raft.GetPeers()
	t := m.raft.GetTransport()
	req := protocol.AppendEntriesRequest{
		Term:              m.raft.GetCurrentTerm(),
		MasterID:          m.raft.GetServerID(),
		PrevLogIndex:      0, // TODO
		PrevLogTerm:       0, // TODO
		LeaderCommitIndex: m.raft.GetCommitIndex(),
		LogEntry:          make([]common.LogEntry, 0),
	}
	broadcastResultChan := make(chan broadcastResult, len(m.raft.GetPeers()))
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			peerTransport := peer.GetTransport()
			resp, err := t.SendAppendEntries(peerTransport, req)
			if err != nil {
				m.raft.Logger().Error("failed to send heartbeat", zap.String("address", peerTransport.GetAddress()), zap.Error(err))
			}
			broadcastResultChan <- broadcastResult{
				followerID: peer.GetServerID(),
				resp:       resp,
				err:        err,
			}
		}()
	}

	go func() {
		wg.Wait()
		close(broadcastResultChan)
	}()
	return broadcastResultChan
}

func (m *Master) HandleClientAppendEntry(request protocol.ClientAppendEntryRequest) (protocol.ClientAppendEntryResponse, error) {
	logEntry, err := m.raft.AppendLogEntry(request.Data)
	if err != nil {
		return protocol.ClientAppendEntryResponse{}, err
	}
	broadcastResultChan := m.broadcastClientAppendEntry(logEntry)
	timeoutTimer := time.NewTimer(time.Duration(500 * time.Millisecond)) // TODO: put the timeout in setting
	defer timeoutTimer.Stop()

	receivedCount := 0
	grantedCount := 1
	for receivedCount < len(m.followers) && grantedCount >= common.GetMajorityCount(len(m.followers)+1) {
		select {
		case broadcastResult := <-broadcastResultChan:
			receivedCount += 1
			if broadcastResult.err != nil {
				m.raft.Logger().Error("failed to broadcast new entry to follower", zap.Int("followerID", int(broadcastResult.followerID)))
				continue
			}
			m.handleAppendEntriesResponse(broadcastResult.followerID, broadcastResult.resp)
			grantedCount += 1
		case <-timeoutTimer.C:
			return protocol.ClientAppendEntryResponse{
				IsSucceeded: false,
			}, ErrBroadcastTimeout
		}
	}
	return protocol.ClientAppendEntryResponse{
		IsSucceeded: true,
	}, nil
}

func (m *Master) Stop() {
	close(m.stopSignal)
	<-m.done
	m.isRunning = false
}

type FollowerInfo struct {
	serverID     common.ServerID
	synced       bool
	lastLogIndex common.LogIndex
	lastLogTerm  common.Term
}

func (m *Master) broadcastClientAppendEntry(logEntry common.LogEntry) chan broadcastResult {
	peers := m.raft.GetPeers()
	resultChan := make(chan broadcastResult, len(peers))
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := m.appendEntries(peer, logEntry)
			if err != nil {
				m.raft.Logger().Error(
					"failed to replicate log to peer = %d, err = %w",
					zap.Int32("follower_id", peer.GetServerID()),
					zap.Error(err),
				)
			}
			resultChan <- broadcastResult{
				followerID: peer.GetServerID(),
				resp:       resp,
				err:        err,
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	return resultChan
}

func (m *Master) appendEntries(peer raft.Peer, clientNewLogEntry common.LogEntry) (protocol.AppendEntriesResponse, error) {
	followerInfo := m.followers[peer.GetServerID()]
	followerTransport := peer.GetTransport()

	// Sync the logs of the follower with the current master.
	if !followerInfo.synced {
		err := m.syncLogWithFollower(followerTransport, followerInfo)
		if err != nil {
			return protocol.AppendEntriesResponse{}, err
		}
	}

	// Replicate logs.
	logEntries, err := m.raft.GetLogEntriesStartAt(followerInfo.lastLogIndex+1, int(clientNewLogEntry.Index-followerInfo.lastLogIndex))
	if err != nil {
		return protocol.AppendEntriesResponse{}, err
	}
	req := protocol.AppendEntriesRequest{
		Term:              m.raft.GetCurrentTerm(),
		MasterID:          m.raft.GetServerID(),
		PrevLogIndex:      followerInfo.lastLogIndex,
		PrevLogTerm:       followerInfo.lastLogTerm,
		LeaderCommitIndex: m.raft.GetCommitIndex(),
		LogEntry:          logEntries,
	}

	masterTransport := m.raft.GetTransport()
	resp, err := masterTransport.SendAppendEntries(followerTransport, req)
	if err != nil {
		return protocol.AppendEntriesResponse{}, err
	}
	return resp, nil
}

func (m *Master) handleAppendEntriesResponse(followerID common.ServerID, resp protocol.AppendEntriesResponse) {
	followerInfo := m.followers[followerID]
	followerInfo.lastLogIndex = resp.LastLogIndex
	followerInfo.lastLogTerm = resp.LastLogTerm
	m.followers[followerID] = followerInfo
}

type syncLogResult struct {
	isSynced bool
	nextReq  protocol.AppendEntriesRequest
}

func (m *Master) syncLogWithFollower(followerTransport transport.Transport, followerInfo FollowerInfo) error {
	masterTransport := m.raft.GetTransport()
	nextReq := protocol.AppendEntriesRequest{
		Term:              m.raft.GetCurrentTerm(),
		MasterID:          m.raft.GetServerID(),
		PrevLogIndex:      followerInfo.lastLogIndex,
		PrevLogTerm:       followerInfo.lastLogTerm,
		LeaderCommitIndex: m.raft.GetCommitIndex(),
		LogEntry:          []common.LogEntry{},
	}
	syncResult := syncLogResult{
		isSynced: false,
		nextReq:  nextReq,
	}
	for !syncResult.isSynced {
		resp, err := masterTransport.SendAppendEntries(followerTransport, syncResult.nextReq)
		if err != nil {
			return fmt.Errorf("failed to send append entries request to sync log %w", err)
		}
		if resp.LastLogIndex >= syncResult.nextReq.PrevLogIndex {
			return fmt.Errorf("LastLogIndex from response is invalid, followerId=%d, resp.LastLogIndex=%d, req.LastLogIndex=%d", followerInfo.serverID, resp.LastLogIndex, syncResult.nextReq.PrevLogIndex)
		}
		syncResult, err = m.determineNextAppendEntriesRequest(resp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Master) determineNextAppendEntriesRequest(resp protocol.AppendEntriesResponse) (syncLogResult, error) {
	logEntries, err := m.raft.GetLogEntriesStartAt(resp.LastLogIndex, 1)
	if err != nil {
		return syncLogResult{}, err
	}
	if len(logEntries) == 1 && logEntries[0].Term == resp.Term {
		return syncLogResult{
			isSynced: true,
		}, nil
	}

	if len(logEntries) == 0 {
		logEntries, err := m.raft.GetLogEntriesStartAt(m.raft.GetCommitIndex(), 1)
		if err != nil {
			return syncLogResult{}, err
		}
		nextReq := protocol.AppendEntriesRequest{
			Term:              m.raft.GetCurrentTerm(),
			MasterID:          m.raft.GetServerID(),
			PrevLogIndex:      resp.LastLogIndex,
			PrevLogTerm:       resp.LastLogTerm,
			LeaderCommitIndex: m.raft.GetCommitIndex(),
			LogEntry:          logEntries,
		}
		return syncLogResult{
			isSynced: false,
			nextReq:  nextReq,
		}, nil
	}

	// Reach here means len(logEntries) == 1 and logEntries[0].Term != resp.Term
	nextReq := protocol.AppendEntriesRequest{
		Term:              m.raft.GetCurrentTerm(),
		MasterID:          m.raft.GetServerID(),
		PrevLogIndex:      resp.LastLogIndex,
		PrevLogTerm:       resp.LastLogTerm,
		LeaderCommitIndex: m.raft.GetCommitIndex(),
		LogEntry:          logEntries,
	}
	return syncLogResult{
		isSynced: false,
		nextReq:  nextReq,
	}, nil
}
