package raft

import (
	"time"

	"go.uber.org/zap"
)

type State interface {
	Run() error
}

type Follower struct {
	IsReachable     bool
	lastHeartBeatAt time.Time
	transport       Transport
	logger          zap.Logger
}

func (f Follower) Run() error {
	eventChan, err := f.transport.StartListening()
	if err != nil {
		f.logger.Fatal("failed to start listening to messages")
		panic(err) // TODO add logging
	}

	go func() {
		for msg := range eventChan {
			if msg.Event == EventHeartBeat {
				appendEntriesRequest, err := ParseMessage[AppendEntriesRequest](msg)
				if err != nil {
					f.logger.Error("failed to parse heartbeat message")
				}

				err = f.handleHeartBeat(appendEntriesRequest)
				if err != nil {
					f.logger.Error("failed to handle heartbeat message")
				}
			} else if msg.Event == EventAppendEntries {

			}
		}
	}()
	return nil
}

func (f Follower) handleHeartBeat(request AppendEntriesRequest) error {
	f.lastHeartBeatAt = time.Now()
	return nil
}

type Master struct {
	// Volatile states for leaders
	nextIndex  []LogIndex
	matchIndex []LogIndex

	transport Transport
	followers []Follower
}
