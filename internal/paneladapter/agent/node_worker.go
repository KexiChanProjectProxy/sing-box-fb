package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/log"
)

type NodeSession interface {
	Recovery(ctx context.Context) error
	PollConfiguration(ctx context.Context) error
	PollUsers(ctx context.Context) error
	ReportTraffic(ctx context.Context) error
	SendHeartbeat(ctx context.Context) error
	Close() error
}

type NodeSessionFactory interface {
	New(ctx context.Context, node contract.AgentManifestNode) (NodeSession, contract.PollIntervals, error)
}

type PollIntervalPolicy struct {
	Minimum time.Duration
	Maximum time.Duration
}

func (policy PollIntervalPolicy) Clamp(seconds int) time.Duration {
	interval := time.Duration(seconds) * time.Second
	minimum := policy.Minimum
	if minimum <= 0 {
		minimum = time.Second
	}
	maximum := policy.Maximum
	if maximum < minimum {
		maximum = 5 * time.Minute
	}
	if interval < minimum {
		return minimum
	}
	if interval > maximum {
		return maximum
	}
	return interval
}

type NodeWorkerOptions struct {
	Sessions  NodeSessionFactory
	Logger    log.ContextLogger
	Intervals PollIntervalPolicy
}

type NodeWorker struct {
	sessions  NodeSessionFactory
	logger    log.ContextLogger
	intervals PollIntervalPolicy
}

func NewNodeWorker(options NodeWorkerOptions) (*NodeWorker, error) {
	if options.Sessions == nil {
		return nil, fmt.Errorf("node session factory is required")
	}
	if options.Logger == nil {
		return nil, fmt.Errorf("node worker logger is required")
	}
	return &NodeWorker{sessions: options.Sessions, logger: options.Logger, intervals: options.Intervals}, nil
}

func (worker *NodeWorker) Run(ctx context.Context, node contract.AgentManifestNode) error {
	session, intervals, err := worker.sessions.New(ctx, node)
	if err != nil {
		return fmt.Errorf("create node %s session: %w", node.NodeID, err)
	}
	defer func() {
		if err := session.Close(); err != nil {
			worker.logger.WarnContext(ctx, "close node session: ", err)
		}
	}()
	if err := session.Recovery(ctx); err != nil {
		worker.logger.WarnContext(ctx, "recover node session: ", err)
	}
	if err := session.PollUsers(ctx); err != nil {
		worker.logger.WarnContext(ctx, "initial user poll: ", err)
	}
	tasks := []struct {
		name     string
		interval time.Duration
		call     func(context.Context) error
	}{
		{name: "configuration", interval: worker.intervals.Clamp(intervals.ConfigurationSeconds), call: session.PollConfiguration},
		{name: "users", interval: worker.intervals.Clamp(intervals.UsersSeconds), call: session.PollUsers},
		{name: "traffic", interval: worker.intervals.Clamp(intervals.TrafficSeconds), call: session.ReportTraffic},
		{name: "heartbeat", interval: worker.intervals.Clamp(intervals.HeartbeatSeconds), call: session.SendHeartbeat},
	}
	var waitGroup sync.WaitGroup
	for _, task := range tasks {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			worker.runPeriodic(ctx, node.NodeID, nodePeriodicTask{Name: task.name, Interval: task.interval, Call: task.call})
		}()
	}
	<-ctx.Done()
	waitGroup.Wait()
	return nil
}

type nodePeriodicTask struct {
	Name     string
	Interval time.Duration
	Call     func(context.Context) error
}

func (worker *NodeWorker) runPeriodic(ctx context.Context, nodeID string, task nodePeriodicTask) {
	ticker := time.NewTicker(task.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := task.Call(ctx); err != nil {
				worker.logger.WarnContext(ctx, "node worker ", nodeID, " ", task.Name, ": ", err)
			}
		}
	}
}

var _ WorkerRunner = (*NodeWorker)(nil)
