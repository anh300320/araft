package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/anh300320/araft/internal/raft/common"
	"github.com/anh300320/araft/internal/raft/protocol"
	"go.uber.org/zap"
)

type HttpTransport struct {
	client   http.Client
	logger   *zap.Logger
	hostName string
	port     int16
	events   chan protocol.EventMessage
	server   *http.Server
}

func NewHttpTransport(logger *zap.Logger, hostname string, port int) *HttpTransport {
	return NewHttpTransportWithAddress(logger, hostname, port)
}

func NewHttpTransportWithAddress(logger *zap.Logger, hostname string, port int) *HttpTransport {
	httpClient := http.Client{
		Timeout: 30 * time.Second,
	}
	return &HttpTransport{
		client:   httpClient,
		logger:   logger,
		hostName: hostname,
		port:     int16(port),
		events:   make(chan protocol.EventMessage),
	}
}

func (t *HttpTransport) SendAppendEntries(other Transport, request protocol.AppendEntriesRequest) (protocol.AppendEntriesResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		msg := fmt.Sprintf("failed to marshal append entries message: %s", err.Error())
		t.logger.Error(msg)
		return protocol.AppendEntriesResponse{IsSucceeded: false}, ErrSerializeMessage
	}

	t.logger.Info(
		"sending append entries",
		zap.String("address", other.GetAddress()),
	)
	var appendEntriesResponse protocol.AppendEntriesResponse
	resp, err := t.client.Post(
		other.GetAddress(),
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		t.logger.Error("failed to send append entries request", zap.Error(err))
		return appendEntriesResponse, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&appendEntriesResponse)
	if err != nil {
		return appendEntriesResponse, fmt.Errorf("failed to send append entries %w", err)
	}
	return appendEntriesResponse, nil
}

func (t *HttpTransport) SendVote(other Transport, request protocol.VoteRequest) (protocol.VoteResponse, error) {
	var voteResponse protocol.VoteResponse
	body, err := json.Marshal(request)
	if err != nil {
		return voteResponse, fmt.Errorf("failed to marshal VoteRequest: %w", err)
	}

	voteEndpointURL, err := common.BuildURL(other.GetAddress(), "/votes")
	if err != nil {
		return voteResponse, fmt.Errorf("failed to build vote endpoint URL: %w", err)
	}

	t.logger.Info(
		"sending votes",
		zap.String("endpoint", voteEndpointURL),
	)

	resp, err := t.client.Post(
		voteEndpointURL,
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		t.logger.Error("failed to send vote request", zap.Error(err))
		return voteResponse, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&voteResponse)
	if err != nil {
		return voteResponse, fmt.Errorf("failed to send vote request %w", err)
	}
	return voteResponse, nil

}

func (t *HttpTransport) GetAddress() string {
	return "http://" + t.hostName + ":" + strconv.Itoa(int(t.port))
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

func (t *HttpTransport) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	handleHttpRequest[protocol.AppendEntriesRequest, protocol.AppendEntriesResponse](t, protocol.EventAppendEntries, w, r)
}

func (t *HttpTransport) handleVote(w http.ResponseWriter, r *http.Request) {
	handleHttpRequest[protocol.VoteRequest, protocol.VoteResponse](t, protocol.EventVote, w, r)
}

func (t *HttpTransport) handlePreVote(w http.ResponseWriter, r *http.Request) {
	handleHttpRequest[protocol.PreVoteRequest, protocol.PreVoteResponse](t, protocol.EventPreVote, w, r)
}

func (t *HttpTransport) handleClientAppendEntry(w http.ResponseWriter, r *http.Request) {
	handleHttpRequest[protocol.ClientAppendEntryRequest, protocol.ClientAppendEntryResponse](t, protocol.EventClientAppendEntry, w, r)
}

func (t *HttpTransport) healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (t *HttpTransport) StartListening() (chan protocol.EventMessage, error) {
	t.events = make(chan protocol.EventMessage)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /prevotes", t.handlePreVote)
	mux.HandleFunc("POST /entries", t.handleAppendEntries)
	mux.HandleFunc("POST /votes", t.handleVote)
	mux.HandleFunc("POST /user_entries", t.handleClientAppendEntry)
	mux.HandleFunc("GET /health", t.healthCheck)

	address := fmt.Sprintf(":%d", t.port)
	t.server = &http.Server{
		Addr:    address,
		Handler: mux,
	}

	go func() {
		t.logger.Info(
			"HTTP Handler running",
			zap.Int16("port", t.port),
		)

		err := t.server.ListenAndServe()
		if err != nil {
			t.logger.Error("failed to start listening", zap.Error(err))
		}
	}()

	return t.events, nil
}

func (t *HttpTransport) SendPreVote(other Transport, request protocol.PreVoteRequest) (protocol.PreVoteResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		msg := fmt.Sprintf("failed to marshal pre-vote message: %s", err.Error())
		t.logger.Error(msg)
		return protocol.PreVoteResponse{}, ErrSerializeMessage
	}
	var preVoteResponse protocol.PreVoteResponse
	preVoteEndpoint, err := common.BuildURL(other.GetAddress(), "/prevotes")
	if err != nil {
		t.logger.Error("failed to form prevote endpoint URL", zap.String("host", other.GetAddress()), zap.Error(err))
		return preVoteResponse, nil
	}

	t.logger.Info(
		"sending pre-votes",
		zap.String("endpoint", preVoteEndpoint),
	)

	resp, err := t.client.Post(
		preVoteEndpoint,
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		t.logger.Error("failed to send pre vote request", zap.Error(err))
		return preVoteResponse, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&preVoteResponse)
	if err != nil {
		return preVoteResponse, fmt.Errorf("failed to send pre vote request %w", err)
	}
	return preVoteResponse, nil
}
