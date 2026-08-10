package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/reporter"
	"github.com/sagernet/sing-box/internal/paneladapter/runtime"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
	"github.com/sagernet/sing-box/internal/paneladapter/users"
	"github.com/sagernet/sing-box/log"
)

type RuntimeSessionFactoryOptions struct {
	Client     *client.Client
	Store      *state.Store
	LogFactory log.Factory
	Logger     log.ContextLogger
}

type RuntimeSessionFactory struct {
	client     *client.Client
	store      *state.Store
	logFactory log.Factory
	logger     log.ContextLogger
}

func NewRuntimeSessionFactory(options RuntimeSessionFactoryOptions) (*RuntimeSessionFactory, error) {
	if options.Client == nil || options.Store == nil || options.LogFactory == nil || options.Logger == nil {
		return nil, fmt.Errorf("runtime session dependencies are required")
	}
	return &RuntimeSessionFactory{
		client:     options.Client,
		store:      options.Store,
		logFactory: options.LogFactory,
		logger:     options.Logger,
	}, nil
}

func (factory *RuntimeSessionFactory) New(ctx context.Context, node contract.AgentManifestNode) (NodeSession, contract.PollIntervals, error) {
	nodeClient, err := factory.client.ForNode(node.NodeID)
	if err != nil {
		return nil, contract.PollIntervals{}, fmt.Errorf("create node client: %w", err)
	}
	nodeStore := state.NewNodeStore(factory.store, node.NodeID)
	tracker := traffic.NewTracker(nil)
	manager, err := runtime.NewManager(nodeClient, nodeStore, tracker, factory.logFactory)
	if err != nil {
		return nil, contract.PollIntervals{}, fmt.Errorf("create runtime manager: %w", err)
	}
	if err := manager.Bootstrap(ctx); err != nil {
		_ = manager.Close()
		return nil, contract.PollIntervals{}, fmt.Errorf("bootstrap node runtime: %w", err)
	}
	updateTrackerMapping(tracker, nodeStore.State())
	report := reporter.NewReporter(
		nodeClient,
		nodeStore,
		tracker,
		node.NodeID,
		reporter.WithLogger(factory.logger),
		reporter.WithConfigRevision(nodeStore.State().Config.Revision),
	)
	session := &runtimeSession{
		client:   nodeClient,
		store:    nodeStore,
		tracker:  tracker,
		manager:  manager,
		reporter: report,
		logger:   factory.logger,
	}
	configuration, _, fetchErr := nodeClient.FetchConfiguration(ctx, "")
	if fetchErr != nil || configuration == nil {
		return session, defaultPollIntervals(), nil
	}
	return session, normalizedPollIntervals(configuration.PollIntervals), nil
}

type runtimeSession struct {
	client   *client.Client
	store    state.Repository
	tracker  *traffic.Tracker
	manager  *runtime.Manager
	reporter *reporter.Reporter
	logger   log.ContextLogger
}

func (session *runtimeSession) Recovery(ctx context.Context) error {
	return session.reporter.Recovery(ctx)
}

func (session *runtimeSession) PollConfiguration(ctx context.Context) error {
	if err := session.manager.PollConfiguration(ctx); err != nil {
		return err
	}
	current := session.store.State()
	updateTrackerMapping(session.tracker, current)
	session.reporter.SetConfigRevision(current.Config.Revision)
	return nil
}

func (session *runtimeSession) PollUsers(ctx context.Context) error {
	boxInstance := session.manager.GetBox()
	if boxInstance == nil {
		return fmt.Errorf("node runtime is not available")
	}
	poller := users.NewPoller(session.client, session.store, boxInstance, session.logger)
	current := session.store.State()
	var pollErrors []error
	for _, inbound := range managedInbounds(current) {
		if err := poller.PollInbound(ctx, inbound.InboundID, inbound, current.Config.Revision); err != nil {
			pollErrors = append(pollErrors, fmt.Errorf("poll inbound %s: %w", inbound.InboundID, err))
		}
	}
	return errors.Join(pollErrors...)
}

func (session *runtimeSession) ReportTraffic(ctx context.Context) error {
	return session.reporter.ReportNow(ctx)
}

func (session *runtimeSession) SendHeartbeat(ctx context.Context) error {
	current := session.store.State()
	inbounds := make([]contract.HeartbeatInbound, 0, len(current.Inbounds))
	for _, inbound := range current.Inbounds {
		status := contract.UserLoadStatus(inbound.UserLoadStatus)
		if status == "" {
			status = contract.UserLoadStatusEmptyInitialLoad
		}
		if !contract.IsSupportedProtocol(inbound.Protocol) {
			status = contract.UserLoadStatusUnsupportedProto
		}
		tag := inbound.Tag
		if tag == "" {
			tag = inbound.InboundID
		}
		inbounds = append(inbounds, contract.HeartbeatInbound{
			Tag: tag, Protocol: inbound.Protocol, Status: status, CurrentUserCount: inbound.UserCount,
		})
	}
	runtimeMetrics := session.tracker.GetRuntimeMetrics()
	runtimeMetrics.UptimeSeconds = int64(time.Since(session.manager.StartTime()).Seconds())
	return session.client.SendHeartbeat(ctx, &contract.Heartbeat{
		ObservedAt:                   time.Now().UTC(),
		SingBoxVersion:               C.Version,
		AdapterVersion:               C.Version,
		AppliedConfigurationRevision: current.Config.Revision,
		InboundStatuses:              inbounds,
		Runtime:                      runtimeMetrics,
	})
}

func (session *runtimeSession) Close() error {
	return session.manager.Close()
}

func updateTrackerMapping(tracker *traffic.Tracker, current *state.State) {
	mapping := make(map[string]string, len(current.Inbounds))
	for _, inbound := range current.Inbounds {
		mapping[inbound.Tag] = inbound.InboundID
	}
	tracker.UpdateInboundMapping(mapping)
}

func managedInbounds(current *state.State) []contract.ManagedInbound {
	inbounds := make([]contract.ManagedInbound, 0, len(current.Inbounds))
	for _, inbound := range current.Inbounds {
		inbounds = append(inbounds, contract.ManagedInbound{
			InboundID: inbound.InboundID, Tag: inbound.Tag, Protocol: inbound.Protocol,
			UserApplyPolicy: contract.ApplyOnUserHotReloadUsers,
		})
	}
	return inbounds
}

func normalizedPollIntervals(intervals contract.PollIntervals) contract.PollIntervals {
	defaults := defaultPollIntervals()
	if intervals.ConfigurationSeconds <= 0 {
		intervals.ConfigurationSeconds = defaults.ConfigurationSeconds
	}
	if intervals.UsersSeconds <= 0 {
		intervals.UsersSeconds = defaults.UsersSeconds
	}
	if intervals.TrafficSeconds <= 0 {
		intervals.TrafficSeconds = defaults.TrafficSeconds
	}
	if intervals.HeartbeatSeconds <= 0 {
		intervals.HeartbeatSeconds = defaults.HeartbeatSeconds
	}
	return intervals
}

func defaultPollIntervals() contract.PollIntervals {
	return contract.PollIntervals{
		ConfigurationSeconds: 60,
		UsersSeconds:         60,
		TrafficSeconds:       60,
		HeartbeatSeconds:     60,
	}
}

var _ NodeSessionFactory = (*RuntimeSessionFactory)(nil)
var _ NodeSession = (*runtimeSession)(nil)
