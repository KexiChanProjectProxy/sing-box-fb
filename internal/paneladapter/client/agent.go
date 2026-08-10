package client

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	E "github.com/sagernet/sing/common/exceptions"
)

func NewAgent(baseURL, agentID, token string, options ...Option) (*Client, error) {
	if agentID == "" {
		return nil, E.New("agentID is required")
	}
	return newClient(baseURL, clientIdentity{agentID: agentID}, token, options...)
}

func (client *Client) ForNode(nodeID string) (*Client, error) {
	if client.agentID == "" {
		return nil, E.New("agent client is required")
	}
	if nodeID == "" {
		return nil, E.New("nodeID is required")
	}
	return &Client{
		baseURL:     client.baseURL,
		agentID:     client.agentID,
		nodeID:      nodeID,
		tokenSource: client,
		httpClient:  client.httpClient,
		logger:      client.logger,
	}, nil
}

func (client *Client) FetchManifest(ctx context.Context, etag string) (*contract.AgentManifest, string, error) {
	if client.agentID == "" {
		return nil, "", E.New("agent client is required")
	}
	request, err := client.newRequest(ctx, http.MethodGet, client.manifestURL(), nil)
	if err != nil {
		return nil, "", err
	}
	if etag != "" {
		request.Header.Set("If-None-Match", etag)
	}
	response, err := client.doRequest(request)
	if err != nil {
		return nil, "", err
	}
	switch response.StatusCode {
	case http.StatusOK:
		if err := checkNoStore(response); err != nil {
			_, _ = readAndClose(response)
			return nil, "", err
		}
		body, err := readAndClose(response)
		if err != nil {
			return nil, "", E.Cause(err, "read agent manifest response")
		}
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		var manifest contract.AgentManifest
		if err := decoder.Decode(&manifest); err != nil {
			return nil, "", E.Cause(err, "decode agent manifest response")
		}
		if err := manifest.Validate(); err != nil {
			return nil, "", E.Cause(err, "validate agent manifest response")
		}
		return &manifest, response.Header.Get("ETag"), nil
	case http.StatusNotModified:
		_, _ = readAndClose(response)
		return nil, "", ErrNotModified
	default:
		return nil, "", client.handleErrorResponse(response)
	}
}

func (client *Client) SendAgentHeartbeat(ctx context.Context, heartbeat *contract.AgentHeartbeat) error {
	if client.agentID == "" {
		return E.New("agent client is required")
	}
	body, err := json.Marshal(heartbeat)
	if err != nil {
		return E.Cause(err, "encode agent heartbeat")
	}
	request, err := client.newRequest(ctx, http.MethodPost, client.agentHeartbeatURL(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.doRequest(request)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return client.handleErrorResponse(response)
	}
	_, err = readAndClose(response)
	if err != nil {
		return E.Cause(err, "read agent heartbeat response")
	}
	return nil
}
