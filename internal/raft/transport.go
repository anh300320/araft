package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"go.uber.org/zap"
)

type Transport interface {
	StartListening() (chan EventMessage, error)
	SendHeartBeat(other Transport, request AppendEntriesRequest) (AppendEntriesResponse, error)
	AppendEntries(other Transport, request AppendEntriesRequest) (AppendEntriesResponse, error)
	SendVote(other Transport, request VoteRequest) (VoteResponse, error)
	GetAddress() string
}

var SerializeMessageError = errors.New("failed to serialize message")
var HeartBeatMessageError = errors.New("failed to send heartbeat")

type HttpTransport struct {
	client   http.Client
	logger   zap.Logger
	hostName string
	port     int16
	events   chan EventMessage
}

func (t HttpTransport) GetAddress() string {
	return t.hostName + strconv.Itoa(int(t.port))
}

func (t HttpTransport) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var appendEntriesRequest AppendEntriesRequest

	err := json.NewDecoder(r.Body).Decode(&appendEntriesRequest)

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}
	bodyString := string(bodyBytes)
	newEvent := EventMessage{
		Event: EventHeartBeat,
		body:  bodyString,
	}
	t.events <- newEvent
}

func (t HttpTransport) StartListening() (chan EventMessage, error) {
	t.events = make(chan EventMessage)
	defer close(t.events)

	http.HandleFunc("/heartbeat", t.handleHeartbeat)
	t.logger.Info(
		"HTTP Handler running",
		zap.Int16("port", t.port),
	)

	address := fmt.Sprintf(":%d", t.port)
	err := http.ListenAndServe(address, nil)
	if err != nil {
		panic(err) // TODO: check this
	}

	return t.events, nil
}

func (t HttpTransport) SendHeartBeat(other Transport, request AppendEntriesRequest) (*AppendEntriesResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		msg := fmt.Sprintf("failed to marshal heartbeat message: %s", err.Error())
		t.logger.Error(msg)
		return nil, SerializeMessageError
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
	var appendEntriesResponse AppendEntriesResponse
	err = json.NewDecoder(resp.Body).Decode(&appendEntriesResponse)
	if err != nil {
		return nil, HeartBeatMessageError
	}
	return &appendEntriesResponse, nil
}
