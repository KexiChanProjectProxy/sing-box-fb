// Package client implements the strict REST v1 client for the panel adapter.
package client

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
)

var ErrNotModified = E.New("resource not modified")

var ErrNoStore = E.New("response missing Cache-Control: no-store")

type ResponseError struct {
	StatusCode int
	Code       string
	Title      string
	Detail     string
	RequestID  string
}

func (e *ResponseError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("panel API error %d %s: %s (%s)", e.StatusCode, e.Code, e.Title, e.Detail)
	}
	return fmt.Sprintf("panel API error %d %s: %s", e.StatusCode, e.Code, e.Title)
}

func IsRetryable(err error) bool {
	retryable, _, _ := ClassifyError(err)
	return retryable
}

func ClassifyError(err error) (retryable bool, isConflict bool, isMismatch bool) {
	if err == nil {
		return false, false, false
	}
	if errors.Is(err, ErrNotModified) || errors.Is(err, ErrNoStore) {
		return false, false, false
	}
	var responseError *ResponseError
	if !errors.As(err, &responseError) {
		return true, false, false
	}
	switch responseError.StatusCode {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return true, false, false
	case http.StatusConflict:
		return false, true, false
	case http.StatusForbidden:
		return false, false, responseError.Code == "NODE_MISMATCH"
	default:
		return false, false, false
	}
}

type Option func(*Client)

func WithHTTPClient(httpClient *http.Client) Option {
	return func(client *Client) {
		client.httpClient = httpClient
	}
}

func WithLogger(contextLogger log.ContextLogger) Option {
	return func(client *Client) {
		client.logger = contextLogger
	}
}

type Client struct {
	baseURL     *url.URL
	agentID     string
	nodeID      string
	tokenMu     sync.RWMutex
	token       string
	tokenSource *Client
	httpClient  *http.Client
	logger      log.ContextLogger
}

type clientIdentity struct {
	agentID string
	nodeID  string
}

func New(baseURL, nodeID, token string, options ...Option) (*Client, error) {
	if nodeID == "" {
		return nil, E.New("nodeID is required")
	}
	return newClient(baseURL, clientIdentity{nodeID: nodeID}, token, options...)
}

func newClient(baseURL string, identity clientIdentity, token string, options ...Option) (*Client, error) {
	if baseURL == "" {
		return nil, E.New("baseURL is required")
	}
	if token == "" {
		return nil, E.New("token is required")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, E.Cause(err, "invalid baseURL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, E.New("baseURL must use http or https scheme")
	}
	if parsed.Host == "" {
		return nil, E.New("baseURL must have a host")
	}
	for key := range parsed.Query() {
		lower := strings.ToLower(key)
		if lower == "token" || lower == "secret" || lower == "key" || lower == "api_key" || lower == "apikey" {
			return nil, E.New("baseURL must not contain token/secret in query parameters — use bearer header authentication")
		}
	}
	panelClient := &Client{
		baseURL:    parsed,
		agentID:    identity.agentID,
		nodeID:     identity.nodeID,
		token:      token,
		httpClient: &http.Client{},
		logger:     logger.NOP(),
	}
	for _, option := range options {
		option(panelClient)
	}
	return panelClient, nil
}
