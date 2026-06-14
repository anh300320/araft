package raft

import "time"

type HeartBeat struct {
	lastBeatAt time.Time
	rateMs     int
}

func (h *HeartBeat) Beat(leader *Raft, followers []Raft) error {

}

func (h *HeartBeat) beat(leader *Raft, followers []Raft, rateMs int) error {
	for {
		heartBeatMessage := AppendEntriesRequest{
			MasterID:     leader.serverID,
			PrevLogIndex: leader.commitId,
			PrevLogTerm:  leader.currentTerm,
			LogEntry:     nil,
		}
		for _, follower := range followers {
			resp, err := leader.transport.SendHeartBeat(follower.transport, heartBeatMessage)
			if err != nil {

			}
		}
		time.Sleep(time.Duration(rateMs) * time.Millisecond)
	}
}
