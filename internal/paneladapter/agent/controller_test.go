package agent

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
)

type fakeWorkerFactory struct {
	mu       sync.Mutex
	started  map[string]int
	stopped  map[string]int
	failNode string
	changed  chan struct{}
}

func (factory *fakeWorkerFactory) Run(ctx context.Context, node contract.AgentManifestNode) error {
	if node.NodeID == factory.failNode {
		return errors.New("bad node")
	}
	factory.mu.Lock()
	factory.started[node.NodeID]++
	factory.mu.Unlock()
	factory.signal()
	<-ctx.Done()
	factory.mu.Lock()
	factory.stopped[node.NodeID]++
	factory.mu.Unlock()
	factory.signal()
	return nil
}

func (factory *fakeWorkerFactory) signal() {
	select {
	case factory.changed <- struct{}{}:
	default:
	}
}

func (factory *fakeWorkerFactory) counts(nodeID string) (int, int) {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	return factory.started[nodeID], factory.stopped[nodeID]
}

func TestAgentMode_ThreeNodeStartup(t *testing.T) {
	// Given
	factory := newFakeWorkerFactory("")
	controller := newTestController(t, factory)
	manifest := testManifest("node-one", "node-two", "node-three")

	// When
	err := controller.Reconcile(context.Background(), manifest)

	// Then
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	for _, nodeID := range []string{"node-one", "node-two", "node-three"} {
		waitForCount(t, factory, nodeID, 1, 0)
	}
	controller.Close()
}

func TestAgentMode_DynamicAddRemove(t *testing.T) {
	// Given
	factory := newFakeWorkerFactory("")
	controller := newTestController(t, factory)
	if err := controller.Reconcile(context.Background(), testManifest("node-one", "node-two")); err != nil {
		t.Fatal(err)
	}
	waitForCount(t, factory, "node-one", 1, 0)
	waitForCount(t, factory, "node-two", 1, 0)

	// When
	err := controller.Reconcile(context.Background(), testManifest("node-two", "node-three"))

	// Then
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	waitForCount(t, factory, "node-one", 1, 1)
	waitForCount(t, factory, "node-two", 1, 0)
	waitForCount(t, factory, "node-three", 1, 0)
	updated := testManifest("node-two", "node-three")
	updated.Nodes[0].AssignmentRevision = 2
	if err := controller.Reconcile(context.Background(), updated); err != nil {
		t.Fatal(err)
	}
	waitForCount(t, factory, "node-two", 2, 1)
	controller.Close()
	controller.Close()
}

func TestAgentMode_BadNodeIsolation(t *testing.T) {
	// Given
	factory := newFakeWorkerFactory("node-bad")
	controller := newTestController(t, factory)

	// When
	err := controller.Reconcile(context.Background(), testManifest("node-good", "node-bad"))

	// Then
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	waitForCount(t, factory, "node-good", 1, 0)
	select {
	case workerErr := <-controller.Errors():
		if workerErr == nil {
			t.Fatal("worker error = nil")
		}
	case <-time.After(time.Second):
		t.Fatal("bad node error was not reported")
	}
	controller.Close()
}

func TestAgentMode_MalformedManifest(t *testing.T) {
	// Given
	factory := newFakeWorkerFactory("")
	controller := newTestController(t, factory)
	manifest := testManifest("node-one", "node-one")

	// When
	err := controller.Reconcile(context.Background(), manifest)

	// Then
	if err == nil {
		t.Fatal("Reconcile() error = nil, want duplicate node rejection")
	}
	if started, _ := factory.counts("node-one"); started != 0 {
		t.Fatalf("starts = %d, want 0", started)
	}
}

func TestAgentMode_RestartRecovery(t *testing.T) {
	// Given
	store, err := state.NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := testManifest("node-one", "node-two")
	if err := store.UpdateAndSave(func(root *state.State) {
		root.Manifest = state.ManifestState{ETag: `"manifest-1"`, Snapshot: *manifest}
	}); err != nil {
		t.Fatal(err)
	}
	factory := newFakeWorkerFactory("")
	controller, err := NewController(ControllerOptions{Store: store, Workers: factory})
	if err != nil {
		t.Fatal(err)
	}

	// When
	err = controller.Restore(context.Background())

	// Then
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	waitForCount(t, factory, "node-one", 1, 0)
	waitForCount(t, factory, "node-two", 1, 0)
	if store.State().Manifest.AppliedAt.IsZero() {
		t.Fatal("restored manifest did not record applied_at")
	}
	controller.Close()
}

func newTestController(t *testing.T, factory WorkerRunner) *Controller {
	t.Helper()
	return newTestControllerWithFetcher(t, factory, nil, nil)
}

func newFakeWorkerFactory(failNode string) *fakeWorkerFactory {
	return &fakeWorkerFactory{
		started:  map[string]int{},
		stopped:  map[string]int{},
		failNode: failNode,
		changed:  make(chan struct{}, 1),
	}
}

func newTestControllerWithFetcher(t *testing.T, factory WorkerRunner, fetcher ManifestFetcher, wait WaitFunc) *Controller {
	t.Helper()
	store, err := state.NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(ControllerOptions{
		Fetcher: fetcher,
		Store:   store,
		Workers: factory,
		Retry:   RetryPolicy{Minimum: 10 * time.Millisecond, Maximum: 40 * time.Millisecond},
		Wait:    wait,
	})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func testManifest(nodeIDs ...string) *contract.AgentManifest {
	nodes := make([]contract.AgentManifestNode, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		nodes = append(nodes, contract.AgentManifestNode{
			NodeID:                nodeID,
			Status:                contract.AgentManifestNodeStatusActive,
			AssignmentRevision:    1,
			ConfigurationResource: "/api/v1/nodes/" + nodeID + "/configuration",
			UserResource:          "/api/v1/nodes/" + nodeID + "/inbounds/" + nodeID + "/users",
		})
	}
	return &contract.AgentManifest{
		APIVersion:       contract.AgentManifestAPIVersion,
		AgentID:          "agent-one",
		ManifestRevision: 1,
		Reconciliation:   contract.AgentManifestReconciliation{FullSnapshot: true, PollAfterSeconds: 30},
		Nodes:            nodes,
	}
}

func waitForCount(t *testing.T, factory *fakeWorkerFactory, nodeID string, wantStarted, wantStopped int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		started, stopped := factory.counts(nodeID)
		if started == wantStarted && stopped == wantStopped {
			return
		}
		select {
		case <-factory.changed:
		case <-deadline.C:
			t.Fatalf("node %s counts = %d/%d, want %d/%d", nodeID, started, stopped, wantStarted, wantStopped)
		}
	}
}

var _ ManifestFetcher = (*sequenceFetcher)(nil)
