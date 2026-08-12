package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
)

type ManifestFetcher interface {
	FetchManifest(ctx context.Context, etag string) (*contract.AgentManifest, string, error)
}

type WorkerRunner interface {
	Run(ctx context.Context, node contract.AgentManifestNode) error
}

type WaitFunc func(ctx context.Context, delay time.Duration) error

type RetryPolicy struct {
	Minimum time.Duration
	Maximum time.Duration
}

func (policy RetryPolicy) Next(current time.Duration) time.Duration {
	minimum := policy.Minimum
	if minimum <= 0 {
		minimum = time.Second
	}
	maximum := policy.Maximum
	if maximum < minimum {
		maximum = 30 * time.Second
	}
	if current < minimum {
		return minimum
	}
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

type ControllerOptions struct {
	Context context.Context
	Fetcher ManifestFetcher
	Store   *state.Store
	Workers WorkerRunner
	Retry   RetryPolicy
	Wait    WaitFunc
}

type Controller struct {
	fetcher ManifestFetcher
	store   *state.Store
	workers WorkerRunner
	retry   RetryPolicy
	wait    WaitFunc

	ctx       context.Context
	cancel    context.CancelFunc
	reconcile sync.Mutex
	mu        sync.Mutex
	running   map[string]*workerHandle
	errors    chan error
	closeOnce sync.Once
}

type workerHandle struct {
	node   contract.AgentManifestNode
	cancel context.CancelFunc
	done   chan struct{}
}

func NewController(options ControllerOptions) (*Controller, error) {
	if options.Store == nil {
		return nil, fmt.Errorf("agent controller store is required")
	}
	if options.Workers == nil {
		return nil, fmt.Errorf("agent controller workers are required")
	}
	wait := options.Wait
	if wait == nil {
		wait = waitFor
	}
	parent := options.Context
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &Controller{
		fetcher: options.Fetcher,
		store:   options.Store,
		workers: options.Workers,
		retry:   options.Retry,
		wait:    wait,
		ctx:     ctx,
		cancel:  cancel,
		running: make(map[string]*workerHandle),
		errors:  make(chan error, contract.MaxAgentManifestNodes),
	}, nil
}

func (controller *Controller) Errors() <-chan error {
	return controller.errors
}

func (controller *Controller) Reconcile(_ context.Context, manifest *contract.AgentManifest) error {
	if err := manifest.Validate(); err != nil {
		return fmt.Errorf("validate agent manifest: %w", err)
	}
	controller.reconcile.Lock()
	defer controller.reconcile.Unlock()
	if err := controller.persistManifest(manifest); err != nil {
		return err
	}
	desired := make(map[string]contract.AgentManifestNode, len(manifest.Nodes))
	for _, node := range manifest.Nodes {
		if node.Status == contract.AgentManifestNodeStatusActive {
			desired[node.NodeID] = node
		}
	}
	controller.mu.Lock()
	toStop := make([]*workerHandle, 0)
	for nodeID, handle := range controller.running {
		node, exists := desired[nodeID]
		if exists && node == handle.node {
			delete(desired, nodeID)
			continue
		}
		handle.cancel()
		toStop = append(toStop, handle)
		delete(controller.running, nodeID)
	}
	controller.mu.Unlock()
	for _, handle := range toStop {
		<-handle.done
	}
	controller.mu.Lock()
	for _, node := range desired {
		controller.startWorkerLocked(node)
	}
	controller.mu.Unlock()
	return nil
}

func (controller *Controller) Close() {
	controller.closeOnce.Do(func() {
		controller.cancel()
		controller.mu.Lock()
		handles := make([]*workerHandle, 0, len(controller.running))
		for nodeID, handle := range controller.running {
			handle.cancel()
			handles = append(handles, handle)
			delete(controller.running, nodeID)
		}
		controller.mu.Unlock()
		for _, handle := range handles {
			<-handle.done
		}
	})
}

func (controller *Controller) persistManifest(manifest *contract.AgentManifest) error {
	return controller.store.UpdateAndSave(func(root *state.State) {
		etag := root.Manifest.ETag
		root.Manifest = state.ManifestState{
			ETag: etag, Snapshot: *manifest, AppliedAt: time.Now().UTC(),
		}
		nodes := make(map[string]state.NodeState, len(manifest.Nodes))
		for _, descriptor := range manifest.Nodes {
			node := root.Nodes[descriptor.NodeID]
			node.Manifest = descriptor
			if node.Config.NodeID == "" {
				node.Config.NodeID = descriptor.NodeID
			}
			if node.Inbounds == nil {
				node.Inbounds = make(map[string]state.InboundState)
			}
			nodes[descriptor.NodeID] = node
		}
		root.Nodes = nodes
	})
}

func (controller *Controller) startWorkerLocked(node contract.AgentManifestNode) {
	ctx, cancel := context.WithCancel(controller.ctx)
	handle := &workerHandle{node: node, cancel: cancel, done: make(chan struct{})}
	controller.running[node.NodeID] = handle
	go controller.supervise(ctx, handle)
}

func (controller *Controller) supervise(ctx context.Context, handle *workerHandle) {
	defer close(handle.done)
	retryDelay := time.Duration(0)
	for {
		err := controller.workers.Run(ctx, handle.node)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			controller.reportError(fmt.Errorf("node %s worker: %w", handle.node.NodeID, err))
		}
		retryDelay = controller.retry.Next(retryDelay)
		if err := controller.wait(ctx, retryDelay); err != nil {
			return
		}
	}
}

func (controller *Controller) reportError(err error) {
	select {
	case controller.errors <- err:
	default:
	}
}
