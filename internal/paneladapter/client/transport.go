package client

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	E "github.com/sagernet/sing/common/exceptions"
)

const (
	pathAgentManifest  = "/api/v1/agents/{agentID}/manifest"
	pathAgentHeartbeat = "/api/v1/agents/{agentID}/heartbeats"
	pathConfiguration  = "/api/v1/nodes/{nodeID}/configuration"
	pathUsers          = "/api/v1/nodes/{nodeID}/inbounds/{inboundID}/users"
	pathTraffic        = "/api/v1/nodes/{nodeID}/traffic-reports"
	pathHeartbeat      = "/api/v1/nodes/{nodeID}/heartbeats"
	pathTokenRotation  = "/api/v1/nodes/{nodeID}/adapter-token/rotate"
)

var secureRand = rand.Reader

func (client *Client) manifestURL() string {
	return client.resourceURL(strings.Replace(pathAgentManifest, "{agentID}", url.PathEscape(client.agentID), 1))
}

func (client *Client) agentHeartbeatURL() string {
	return client.resourceURL(strings.Replace(pathAgentHeartbeat, "{agentID}", url.PathEscape(client.agentID), 1))
}

func (client *Client) configurationURL() string {
	return client.resourceURL(strings.Replace(pathConfiguration, "{nodeID}", url.PathEscape(client.nodeID), 1))
}

func (client *Client) usersURL(inboundID string) string {
	path := strings.Replace(pathUsers, "{nodeID}", url.PathEscape(client.nodeID), 1)
	return client.resourceURL(strings.Replace(path, "{inboundID}", url.PathEscape(inboundID), 1))
}

func (client *Client) trafficURL() string {
	return client.resourceURL(strings.Replace(pathTraffic, "{nodeID}", url.PathEscape(client.nodeID), 1))
}

func (client *Client) heartbeatURL() string {
	return client.resourceURL(strings.Replace(pathHeartbeat, "{nodeID}", url.PathEscape(client.nodeID), 1))
}

func (client *Client) tokenRotationURL() string {
	if client.agentID != "" {
		return client.resourceURL("/api/v1/agents/" + url.PathEscape(client.agentID) + "/adapter-token/rotate")
	}
	return client.resourceURL(strings.Replace(pathTokenRotation, "{nodeID}", url.PathEscape(client.nodeID), 1))
}

func (client *Client) resourceURL(path string) string {
	resource := *client.baseURL
	resource.Path = path
	resource.RawQuery = ""
	resource.Fragment = ""
	return resource.String()
}

func generateRequestID() string {
	var buffer [16]byte
	_, _ = io.ReadFull(secureRand, buffer[:])
	return fmt.Sprintf("%x", buffer[:])
}

func (client *Client) newRequest(ctx context.Context, method, urlString string, body io.Reader) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, method, urlString, body)
	if err != nil {
		return nil, E.Cause(err, "create request")
	}
	request.Header.Set("Authorization", "Bearer "+client.currentToken())
	if client.agentID != "" {
		request.Header.Set("X-Agent-ID", client.agentID)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-Request-ID", generateRequestID())
	return request, nil
}

func (client *Client) currentToken() string {
	if client.tokenSource != nil {
		return client.tokenSource.currentToken()
	}
	client.tokenMu.RLock()
	defer client.tokenMu.RUnlock()
	return client.token
}

func (client *Client) SetToken(token string) error {
	if token == "" {
		return E.New("token is required")
	}
	if client.tokenSource != nil {
		return client.tokenSource.SetToken(token)
	}
	client.tokenMu.Lock()
	client.token = token
	client.tokenMu.Unlock()
	return nil
}

func (client *Client) doRequest(request *http.Request) (*http.Response, error) {
	client.logger.DebugContext(request.Context(), "panel client: ", request.Method, " ", redactURL(request.URL))
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, E.Cause(err, "panel request failed")
	}
	return response, nil
}

func readAndClose(response *http.Response) ([]byte, error) {
	defer response.Body.Close()
	return io.ReadAll(response.Body)
}

func (client *Client) handleErrorResponse(response *http.Response) error {
	body, err := readAndClose(response)
	if err != nil {
		return E.Cause(err, "read error response body")
	}
	var problem contract.ProblemDetails
	if jsonErr := json.Unmarshal(body, &problem); jsonErr == nil && problem.Title != "" {
		requestID := problem.RequestID
		if requestID == "" {
			requestID = response.Header.Get("X-Request-ID")
		}
		return &ResponseError{
			StatusCode: response.StatusCode,
			Code:       problem.Code,
			Title:      client.redactResponseText(problem.Title),
			Detail:     client.redactResponseText(problem.Detail),
			RequestID:  requestID,
		}
	}
	return &ResponseError{
		StatusCode: response.StatusCode,
		Code:       fmt.Sprintf("HTTP_%d", response.StatusCode),
		Title:      http.StatusText(response.StatusCode),
		RequestID:  response.Header.Get("X-Request-ID"),
	}
}

func (client *Client) redactResponseText(value string) string {
	secret := client.currentToken()
	if secret == "" {
		return value
	}
	return strings.ReplaceAll(value, secret, "[REDACTED]")
}

func checkNoStore(response *http.Response) error {
	if !strings.Contains(strings.ToLower(response.Header.Get("Cache-Control")), "no-store") {
		return ErrNoStore
	}
	return nil
}

func redactURL(resource *url.URL) string {
	safe := *resource
	safe.User = nil
	safe.RawQuery = ""
	safe.Fragment = ""
	return safe.String()
}
