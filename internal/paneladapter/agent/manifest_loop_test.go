package agent

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
)

type sequenceFetcher struct {
	mu       sync.Mutex
	errors   []error
	manifest *contract.AgentManifest
	calls    int
}

func (fetcher *sequenceFetcher) FetchManifest(context.Context, string) (*contract.AgentManifest, string, error) {
	fetcher.mu.Lock()
	defer fetcher.mu.Unlock()
	fetcher.calls++
	if len(fetcher.errors) > 0 {
		err := fetcher.errors[0]
		fetcher.errors = fetcher.errors[1:]
		return nil, "", err
	}
	return fetcher.manifest, `"manifest-1"`, nil
}

func TestAgentMode_NetworkPartition(t *testing.T) {
	// Given
	factory := newFakeWorkerFactory("")
	fetcher := &sequenceFetcher{
		errors:   []error{errors.New("offline"), errors.New("still offline"), errors.New("offline again"), errors.New("offline remains")},
		manifest: testManifest("node-one"),
	}
	waits := make(chan time.Duration, 6)
	ctx, cancel := context.WithCancel(context.Background())
	controller := newTestControllerWithFetcher(t, factory, fetcher, func(_ context.Context, delay time.Duration) error {
		waits <- delay
		if fetcher.calls >= 5 {
			cancel()
		}
		return nil
	})

	// When
	err := controller.Run(ctx)

	// Then
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond}
	for index, expected := range want {
		if got := <-waits; got != expected {
			t.Fatalf("backoff[%d] = %v, want %v", index, got, expected)
		}
	}
	controller.Close()
}

func TestAgentMode_Manifest304(t *testing.T) {
	// Given
	store, err := state.NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := testManifest("node-one")
	if err := store.UpdateAndSave(func(root *state.State) {
		root.Manifest = state.ManifestState{ETag: `"manifest-1"`, Snapshot: *manifest}
	}); err != nil {
		t.Fatal(err)
	}
	factory := newFakeWorkerFactory("")
	fetcher := &sequenceFetcher{errors: []error{client.ErrNotModified}}
	ctx, cancel := context.WithCancel(context.Background())
	var delay time.Duration
	controller, err := NewController(ControllerOptions{
		Store: store, Workers: factory, Fetcher: fetcher,
		Wait: func(_ context.Context, got time.Duration) error {
			delay = got
			cancel()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// When
	err = controller.Run(ctx)

	// Then
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
	if delay != 30*time.Second {
		t.Fatalf("304 poll delay = %v", delay)
	}
	waitForCount(t, factory, "node-one", 1, 1)
}

var _ ManifestFetcher = (*sequenceFetcher)(nil)
