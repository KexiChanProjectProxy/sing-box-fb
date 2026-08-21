package client

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	E "github.com/sagernet/sing/common/exceptions"
)

func (client *Client) FetchConfiguration(ctx context.Context, etag string) (*contract.ConfigurationResponse, string, error) {
	request, err := client.newRequest(ctx, http.MethodGet, client.configurationURL(), nil)
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
			return nil, "", E.Cause(err, "read configuration response")
		}
		var configuration contract.ConfigurationResponse
		if err := json.Unmarshal(body, &configuration); err != nil {
			return nil, "", E.Cause(err, "decode configuration response")
		}
		return &configuration, response.Header.Get("ETag"), nil
	case http.StatusNotModified:
		_, _ = readAndClose(response)
		return nil, "", ErrNotModified
	default:
		return nil, "", client.handleErrorResponse(response)
	}
}

func (client *Client) FetchUsers(ctx context.Context, inboundID, etag, appliedConfigRevision string) (*contract.UserSnapshot, string, error) {
	request, err := client.newRequest(ctx, http.MethodGet, client.usersURL(inboundID), nil)
	if err != nil {
		return nil, "", err
	}
	if etag != "" {
		request.Header.Set("If-None-Match", etag)
	}
	if appliedConfigRevision != "" {
		request.Header.Set("X-Applied-Configuration-Revision", appliedConfigRevision)
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
			return nil, "", E.Cause(err, "read users response")
		}
		var snapshot contract.UserSnapshot
		if err := json.Unmarshal(body, &snapshot); err != nil {
			return nil, "", E.Cause(err, "decode users response")
		}
		return &snapshot, response.Header.Get("ETag"), nil
	case http.StatusNotModified:
		_, _ = readAndClose(response)
		return nil, "", ErrNotModified
	default:
		return nil, "", client.handleErrorResponse(response)
	}
}

func (client *Client) ReportTraffic(ctx context.Context, report *contract.TrafficReport, idempotencyKey string) error {
	body, err := json.Marshal(report)
	if err != nil {
		return E.Cause(err, "marshal traffic report")
	}
	request, err := client.newRequest(ctx, http.MethodPost, client.trafficURL(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	if report.ConfigurationRevision != "" {
		request.Header.Set("X-Applied-Configuration-Revision", report.ConfigurationRevision)
	}
	response, err := client.doRequest(request)
	if err != nil {
		return err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
		_, _ = readAndClose(response)
		return nil
	default:
		return client.handleErrorResponse(response)
	}
}

func (client *Client) SendHeartbeat(ctx context.Context, heartbeat *contract.Heartbeat) error {
	body, err := json.Marshal(heartbeat)
	if err != nil {
		return E.Cause(err, "marshal heartbeat")
	}
	request, err := client.newRequest(ctx, http.MethodPost, client.heartbeatURL(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if heartbeat.AppliedConfigurationRevision != "" {
		request.Header.Set("X-Applied-Configuration-Revision", heartbeat.AppliedConfigurationRevision)
	}
	response, err := client.doRequest(request)
	if err != nil {
		return err
	}
	switch response.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
		_, _ = readAndClose(response)
		return nil
	default:
		return client.handleErrorResponse(response)
	}
}

type TokenRotationResponse struct {
	Token              string    `json:"token"`
	ExpiresAt          time.Time `json:"expires_at"`
	RotateAfterSeconds int       `json:"rotate_after_seconds"`
}

func (client *Client) RotateToken(ctx context.Context) (*TokenRotationResponse, error) {
	request, err := client.newRequest(ctx, http.MethodPost, client.tokenRotationURL(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusCreated {
		return nil, client.handleErrorResponse(response)
	}
	body, err := readAndClose(response)
	if err != nil {
		return nil, E.Cause(err, "read token rotation response")
	}
	var rotated TokenRotationResponse
	if err := json.Unmarshal(body, &rotated); err != nil {
		return nil, E.Cause(err, "decode token rotation response")
	}
	if rotated.Token == "" || rotated.ExpiresAt.IsZero() {
		return nil, E.New("invalid token rotation response")
	}
	return &rotated, nil
}
