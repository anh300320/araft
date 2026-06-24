package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/anh300320/araft/internal/raft/protocol"
	"go.uber.org/zap"
)

type HttpTransport struct {
	client   http.Client
	logger   zap.Logger
	hostName string
	port     int16
	events   chan protocol.EventMessage
}

func (t *HttpTransport) AppendEntries(other Transport, request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (t *HttpTransport) SendVote(other Transport, request protocol.VoteRequest) (protocol.VoteResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (t *HttpTransport) GetAddress() string {
	return t.hostName + strconv.Itoa(int(t.port))
}

func handleHttpRequest[TReq any, TRes any](t *HttpTransport, event protocol.Event, w http.ResponseWriter, r *http.Request) {
	var request TReq

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}
	newEvent := protocol.EventMessage{
		Event:        event,
		Body:         request,
		ResponseChan: make(chan any),
	}
	defer close(newEvent.ResponseChan)
	t.events <- newEvent
	resp := <-newEvent.ResponseChan
	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(resp.(TRes))
	if err != nil {
		http.Error(w, "cannot encode response", http.StatusInternalServerError)
		return
	}
}

func (t *HttpTransport) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	handleHttpRequest[protocol.AppendEntriesRequest, protocol.AppendEntriesResponse](t, protocol.EventHeartBeat, w, r)
}

func (t *HttpTransport) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	handleHttpRequest[protocol.AppendEntriesRequest, protocol.AppendEntriesResponse](t, protocol.EventAppendEntries, w, r)
}

func (t *HttpTransport) handleVote(w http.ResponseWriter, r *http.Request) {
	handleHttpRequest[protocol.VoteRequest, protocol.VoteResponse](t, protocol.EventVote, w, r)
}

func (t *HttpTransport) handlePreVote(w http.ResponseWriter, r *http.Request) {
	handleHttpRequest[protocol.PreVoteRequest, protocol.PreVoteResponse](t, protocol.EventPreVote, w, r)
}

func (t *HttpTransport) StartListening() (chan protocol.EventMessage, error) {
	t.events = make(chan protocol.EventMessage)
	defer close(t.events)

	http.HandleFunc("/heartbeats", t.handleHeartbeat)
	http.HandleFunc("/prevotes", t.handlePreVote)
	http.HandleFunc("/entries", t.handleAppendEntries)
	http.HandleFunc("/votes", t.handleVote)

	go func() {
		address := fmt.Sprintf(":%d", t.port)
		t.logger.Info(
			"HTTP Handler running",
			zap.Int16("port", t.port),
		)
		err := http.ListenAndServe(address, nil)
		if err != nil {
			panic(err) // TODO: check this, learn panic
		}
	}()

	return t.events, nil
}

func (t *HttpTransport) SendHeartBeat(other Transport, request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		msg := fmt.Sprintf("failed to marshal heartbeat message: %s", err.Error())
		t.logger.Error(msg)
		return protocol.AppendEntriesResponse{IsSucceeded: false}, SerializeMessageError
	}

	t.logger.Info(
		"sending heartbeat",
		zap.String("address", other.GetAddress()),
	)
	resp, err := t.client.Post(
		other.GetAddress(),
		"application/json",
		bytes.NewBuffer(body),
	)
	defer resp.Body.Close()
	var appendEntriesResponse protocol.AppendEntriesResponse
	err = json.NewDecoder(resp.Body).Decode(&appendEntriesResponse)
	if err != nil {
		return appendEntriesResponse, HeartBeatMessageError
	}
	return appendEntriesResponse, nil
}

func (t *HttpTransport) SendPreVote(other Transport, request protocol.PreVoteRequest) (protocol.PreVoteResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		msg := fmt.Sprintf("failed to marshal pre-vote message: %s", err.Error())
		t.logger.Error(msg)
		return protocol.PreVoteResponse{}, SerializeMessageError
	}
	t.logger.Info(
		"sending pre-votes",
		zap.String("address", other.GetAddress()),
	)
	resp, err := t.client.Post(
		other.GetAddress(),
		"application/json",
		bytes.NewBuffer(body),
	)
	defer resp.Body.Close()
	var preVoteResponse protocol.PreVoteResponse
	err = json.NewDecoder(resp.Body).Decode(&preVoteResponse)
	if err != nil {
		return preVoteResponse, HeartBeatMessageError
	}
	return preVoteResponse, nil
}
