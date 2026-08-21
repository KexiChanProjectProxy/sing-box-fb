package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/log"
)

type fakeSessionFactory struct {
	session *fakeNodeSession
}

func (factory fakeSessionFactory) New(context.Context, contract.AgentManifestNode) (NodeSession, contract.PollIntervals, error) {
	return factory.session, contract.PollIntervals{
		ConfigurationSeconds: 1,
		UsersSeconds:         1,
		TrafficSeconds:       1,
		HeartbeatSeconds:     1,
	}, nil
}

type fakeNodeSession struct {
	mu       sync.Mutex
	calls    map[string]int
	observed chan string
}

func (session *fakeNodeSession) record(name string) error {
	session.mu.Lock()
	session.calls[name]++
	session.mu.Unlock()
	select {
	case session.observed <- name:
	default:
	}
	return nil
}

func (session *fakeNodeSession) Recovery(context.Context) error { return session.record("recovery") }
func (session *fakeNodeSession) PollConfiguration(context.Context) error {
	return session.record("config")
}
func (session *fakeNodeSession) PollUsers(context.Context) error { return session.record("users") }
func (session *fakeNodeSession) ReportTraffic(context.Context) error {
	return session.record("traffic")
}
func (session *fakeNodeSession) SendHeartbeat(context.Context) error {
	return session.record("heartbeat")
}
func (session *fakeNodeSession) Close() error { return session.record("close") }

func TestAgentMode_PerNodeWorkers(t *testing.T) {
	// Given
	session := &fakeNodeSession{calls: map[string]int{}, observed: make(chan string, 8)}
	worker, err := NewNodeWorker(NodeWorkerOptions{
		Sessions: fakeSessionFactory{session: session},
		Logger:   log.NewNOPFactory().Logger(),
		Intervals: PollIntervalPolicy{
			Minimum: time.Millisecond,
			Maximum: 5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx, testManifest("node-one").Nodes[0])
	}()

	// When
	want := map[string]bool{"recovery": true, "config": true, "users": true, "traffic": true, "heartbeat": true}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(want) > 0 {
		select {
		case call := <-session.observed:
			delete(want, call)
		case <-deadline.C:
			t.Fatalf("missing worker calls: %#v", want)
		}
	}
	cancel()

	// Then
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatalf("Run() error = %v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("node worker did not stop")
	}
	if session.calls["close"] != 1 {
		t.Fatalf("close calls = %d", session.calls["close"])
	}
}

var _ NodeSessionFactory = fakeSessionFactory{}
var _ NodeSession = (*fakeNodeSession)(nil)
