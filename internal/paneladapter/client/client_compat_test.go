package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSendHeartbeat_acceptsCreatedStatus_whenPanelRecordsHeartbeat(t *testing.T) {
	// Given
	h := &captureHandler{statusCode: http.StatusCreated}
	server := httptest.NewServer(h)
	defer server.Close()
	c, err := newTestClient(server.URL)
	if err != nil {
		t.Fatalf("unexpected client error: %v", err)
	}

	// When
	err = c.SendHeartbeat(context.Background(), validHeartbeat())

	// Then
	if err != nil {
		t.Fatalf("unexpected heartbeat error: %v", err)
	}
}
