package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/config"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/reporter"
	"github.com/sagernet/sing-box/internal/paneladapter/runtime"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
	"github.com/sagernet/sing-box/internal/paneladapter/users"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"

	"github.com/spf13/cobra"
)

var configPath string

const (
	defaultTokenRotationInterval = 6 * time.Hour
	tokenRotationRetryInterval   = 5 * time.Minute
)

var rootCommand = &cobra.Command{
	Use:   "sing-box-panel-adapter",
	Short: "Panel adapter for sing-box",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runAdapter(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCommand.PersistentFlags().StringVarP(&configPath, "config", "c", "", "adapter configuration file path")
	_ = rootCommand.MarkPersistentFlagRequired("config")
}

func main() {
	if err := rootCommand.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// runAdapter is the main lifecycle entry point.
func runAdapter() error {
	// -----------------------------------------------------------------------
	// Step 1: Load and validate local adapter config BEFORE any network calls.
	// -----------------------------------------------------------------------
	cfg, err := config.Load(configPath)
	if err != nil {
		return E.Cause(err, "load adapter config")
	}

	// -----------------------------------------------------------------------
	// Step 2: Initialize logging using sing-box log factory.
	// -----------------------------------------------------------------------
	logLevel := cfg.LogLevel
	if logLevel == "" {
		logLevel = "info"
	}
	logFactory, err := log.New(log.Options{
		Context: context.Background(),
		Options: option.LogOptions{
			Level:     logLevel,
			Output:    "stderr",
			Timestamp: true,
		},
	})
	if err != nil {
		return E.Cause(err, "create log factory")
	}
	if err := logFactory.Start(); err != nil {
		return E.Cause(err, "start log factory")
	}
	defer func() {
		_ = logFactory.Close()
	}()
	logger := logFactory.NewLogger("panel/adapter")

	// -----------------------------------------------------------------------
	// Step 3: Create include context (registers all protocol registries).
	// -----------------------------------------------------------------------
	ctx := include.Context(context.Background())

	// -----------------------------------------------------------------------
	// Step 4: Create panel client.
	// -----------------------------------------------------------------------
	panelClient, err := client.New(
		cfg.PanelBaseURL,
		cfg.NodeID,
		cfg.NodeToken,
		client.WithLogger(logger),
	)
	if err != nil {
		return E.Cause(err, "create panel client")
	}

	logger.Info("panel client created for node_id=", cfg.NodeID)

	// -----------------------------------------------------------------------
	// Step 5: Create state store.
	// -----------------------------------------------------------------------
	store, err := state.NewStore(cfg.StatePath)
	if err != nil {
		return E.Cause(err, "create state store")
	}

	// -----------------------------------------------------------------------
	// Step 6: Create traffic tracker (inbound mapping populated after bootstrap).
	// -----------------------------------------------------------------------
	tracker := traffic.NewTracker(nil)

	// -----------------------------------------------------------------------
	// Step 7: Create runtime manager.
	// -----------------------------------------------------------------------
	manager, err := runtime.NewManager(panelClient, store, tracker, logFactory)
	if err != nil {
		return E.Cause(err, "create runtime manager")
	}

	// -----------------------------------------------------------------------
	// Step 8: Bootstrap — fetch initial config and create Box.
	// -----------------------------------------------------------------------
	if err := manager.Bootstrap(ctx); err != nil {
		return E.Cause(err, "bootstrap")
	}

	// After bootstrap, update the traffic tracker's inbound mapping from state.
	curState := store.State()
	inboundMapping := make(map[string]string, len(curState.Inbounds))
	for _, ib := range curState.Inbounds {
		inboundMapping[ib.Tag] = ib.InboundID
	}
	tracker.UpdateInboundMapping(inboundMapping)

	logger.Info("bootstrap complete, revision=", curState.Config.Revision)

	startupConfig, pollIntervals := getStartupConfiguration(panelClient, ctx, logger)

	// 9. Create user poller.
	boxInstance := manager.GetBox()
	poller := users.NewPoller(panelClient, store, boxInstance, logger)

	// 10. Create traffic reporter.
	rep := reporter.NewReporter(
		panelClient,
		store,
		tracker,
		cfg.NodeID,
		reporter.WithLogger(logger),
		reporter.WithConfigRevision(curState.Config.Revision),
	)

	// Perform reporter recovery for any pending reports from previous runs.
	if err := rep.Recovery(ctx); err != nil {
		logger.Warn("reporter recovery: ", err)
	}

	// -----------------------------------------------------------------------
	// Step 10: Setup context with cancellation for goroutine lifecycle.
	// -----------------------------------------------------------------------
	adapterCtx, adapterCancel := context.WithCancel(ctx)
	defer adapterCancel()

	// -----------------------------------------------------------------------
	// Step 11: Start polling/reporting goroutines.
	// -----------------------------------------------------------------------
	var wg sync.WaitGroup

	// Configuration polling goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		runPeriodic(adapterCtx, "config-poll", time.Duration(pollIntervals.ConfigurationSeconds)*time.Second, logger, func(ctx context.Context) error {
			if err := manager.PollConfiguration(ctx); err != nil {
				return E.Cause(err, "poll configuration")
			}
			// Update traffic tracker mapping after config change.
			st := store.State()
			m := make(map[string]string, len(st.Inbounds))
			for _, ib := range st.Inbounds {
				m[ib.Tag] = ib.InboundID
			}
			tracker.UpdateInboundMapping(m)
			// Update reporter config revision.
			rep.SetConfigRevision(st.Config.Revision)
			return nil
		})
	}()

	managedInbounds := startupConfig.ManagedInbounds
	if len(managedInbounds) == 0 {
		managedInbounds = managedInboundsFromState(curState)
	}
	for _, managedInbound := range managedInbounds {
		if !contract.IsSupportedProtocol(managedInbound.Protocol) {
			continue
		}
		inboundID := managedInbound.InboundID
		wg.Add(1)
		go func(inboundID string, mi contract.ManagedInbound) {
			defer wg.Done()
			runPeriodic(adapterCtx, "user-poll/"+inboundID, time.Duration(pollIntervals.UsersSeconds)*time.Second, logger, func(ctx context.Context) error {
				configRev := store.State().Config.Revision
				return poller.PollInbound(ctx, inboundID, mi, configRev)
			})
		}(inboundID, managedInbound)
	}

	// Traffic reporting goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		runPeriodic(adapterCtx, "traffic-report", time.Duration(pollIntervals.TrafficSeconds)*time.Second, logger, func(ctx context.Context) error {
			if err := rep.ReportNow(ctx); err != nil {
				return E.Cause(err, "report traffic")
			}
			return nil
		})
	}()

	// Heartbeat goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		runPeriodic(adapterCtx, "heartbeat", time.Duration(pollIntervals.HeartbeatSeconds)*time.Second, logger, func(ctx context.Context) error {
			return sendHeartbeat(ctx, panelClient, store, tracker, manager, cfg.NodeID, logger)
		})
	}()

	// Adapter-token rotation goroutine.
	rotationInterval := cfg.TokenRotationInterval.Duration
	if rotationInterval <= 0 {
		rotationInterval = defaultTokenRotationInterval
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		runTokenRotation(adapterCtx, panelClient, configPath, rotationInterval, logger)
	}()

	logger.Info("adapter running — all goroutines started")

	// -----------------------------------------------------------------------
	// Step 12: Handle signals.
	// -----------------------------------------------------------------------
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(osSignals)

	for {
		sig := <-osSignals
		if sig == syscall.SIGHUP {
			logger.Info("received SIGHUP — re-reading local adapter config")
			if newCfg, err := config.Load(configPath); err != nil {
				logger.Warn("reload adapter config: ", err)
			} else {
				newLevel := newCfg.LogLevel
				if newLevel == "" {
					newLevel = "info"
				}
				level, err := log.ParseLevel(newLevel)
				if err != nil {
					logger.Warn("parse log level: ", err)
				} else {
					logFactory.SetLevel(level)
				}
				cfg = newCfg
				if err := panelClient.SetToken(newCfg.NodeToken); err != nil {
					logger.Warn("reload adapter token: ", err)
				}
				logger.Info("adapter config reloaded")
			}
			continue
		}
		// SIGINT or SIGTERM — graceful shutdown.
		logger.Info("received ", sig, " — shutting down")
		adapterCancel()
		break
	}

	// -----------------------------------------------------------------------
	// Step 13: Graceful shutdown.
	// -----------------------------------------------------------------------
	// Wait for goroutines to finish (they respect context cancellation).
	wg.Wait()

	// Flush final state.
	if err := store.Save(); err != nil {
		logger.Warn("flush state on shutdown: ", err)
	}

	// Close the Box.
	if err := manager.Close(); err != nil {
		logger.Warn("close box: ", err)
	}

	// Force-exit after timeout to avoid hanging on stubborn connections.
	go func() {
		time.Sleep(C.FatalStopTimeout)
		os.Exit(1)
	}()

	debug.FreeOSMemory()
	logger.Info("adapter stopped")
	return nil
}

func runTokenRotation(
	ctx context.Context,
	panelClient *client.Client,
	path string,
	interval time.Duration,
	logger log.ContextLogger,
) {
	runTokenRotationWithRetry(ctx, panelClient, path, interval, tokenRotationRetryInterval, logger)
}

func runTokenRotationWithRetry(
	ctx context.Context,
	panelClient *client.Client,
	path string,
	interval time.Duration,
	retryInterval time.Duration,
	logger log.ContextLogger,
) {
	if interval <= 0 {
		interval = defaultTokenRotationInterval
	}
	if retryInterval <= 0 {
		retryInterval = tokenRotationRetryInterval
	}
	next := interval
	var pending *client.TokenRotationResponse
	timer := time.NewTimer(next)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if pending == nil {
				rotated, err := panelClient.RotateToken(ctx)
				if err != nil {
					logger.Warn("token-rotation: ", err)
					next = retryInterval
					timer.Reset(next)
					continue
				}
				pending = rotated
			}
			if err := config.UpdateNodeToken(path, pending.Token); err != nil {
				logger.Warn("token-rotation persist config: ", err)
				next = retryInterval
			} else if err := panelClient.SetToken(pending.Token); err != nil {
				logger.Warn("token-rotation activate token: ", err)
				next = retryInterval
			} else {
				next = interval
				if pending.RotateAfterSeconds > 0 {
					next = time.Duration(pending.RotateAfterSeconds) * time.Second
				}
				logger.Info("adapter token rotated, expires_at=", pending.ExpiresAt.UTC().Format(time.RFC3339))
				pending = nil
			}
			timer.Reset(next)
		}
	}
}

// ---------------------------------------------------------------------------
// Periodic goroutine runner
// ---------------------------------------------------------------------------

// runPeriodic executes fn at the given interval until ctx is cancelled.
// On error, it logs and continues. It uses a jitter-free ticker.
func runPeriodic(ctx context.Context, name string, interval time.Duration, logger log.ContextLogger, fn func(context.Context) error) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := fn(ctx); err != nil {
				logger.Warn(name, ": ", err)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Heartbeat (inline T11 logic)
// ---------------------------------------------------------------------------

// sendHeartbeat builds and sends a heartbeat to the panel.
func sendHeartbeat(
	ctx context.Context,
	panelClient *client.Client,
	store *state.Store,
	tracker *traffic.Tracker,
	manager *runtime.Manager,
	nodeID string,
	logger log.ContextLogger,
) error {
	st := store.State()

	// Build heartbeat inbounds from state.
	inbounds := make([]contract.HeartbeatInbound, 0, len(st.Inbounds))
	for _, ib := range st.Inbounds {
		status := contract.UserLoadStatus(ib.UserLoadStatus)
		tag := ib.Tag
		if tag == "" {
			tag = ib.InboundID
		}
		if status == "" {
			status = contract.UserLoadStatusEmptyInitialLoad
		}
		if !contract.IsSupportedProtocol(ib.Protocol) {
			status = contract.UserLoadStatusUnsupportedProto
		}
		inbounds = append(inbounds, contract.HeartbeatInbound{
			Tag:              tag,
			Protocol:         ib.Protocol,
			Status:           status,
			CurrentUserCount: ib.UserCount,
		})
	}

	// Build runtime metrics.
	runtimeMetrics := tracker.GetRuntimeMetrics()
	runtimeMetrics.UptimeSeconds = int64(time.Since(manager.StartTime()).Seconds())

	hb := &contract.Heartbeat{
		ObservedAt:                   time.Now().UTC(),
		SingBoxVersion:               C.Version,
		AdapterVersion:               C.Version,
		AppliedConfigurationRevision: st.Config.Revision,
		InboundStatuses:              inbounds,
		Runtime:                      runtimeMetrics,
	}

	if err := panelClient.SendHeartbeat(ctx, hb); err != nil {
		return E.Cause(err, "send heartbeat")
	}

	logger.Debug("heartbeat sent, revision=", st.Config.Revision)
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// getPollIntervals fetches the configuration from the panel to obtain poll
// intervals. Since Bootstrap already fetched once, we try to use the
// manager's cached config. Falls back to sensible defaults.
func getStartupConfiguration(panelClient *client.Client, ctx context.Context, logger log.ContextLogger) (*contract.ConfigurationResponse, contract.PollIntervals) {
	cfg, _, err := panelClient.FetchConfiguration(ctx, "")
	if err != nil {
		logger.Warn("fetch poll intervals failed, using defaults: ", err)
		return &contract.ConfigurationResponse{}, defaultPollIntervals()
	}
	if cfg.PollIntervals.ConfigurationSeconds <= 0 ||
		cfg.PollIntervals.UsersSeconds <= 0 ||
		cfg.PollIntervals.TrafficSeconds <= 0 ||
		cfg.PollIntervals.HeartbeatSeconds <= 0 {
		logger.Warn("panel returned invalid poll intervals, using defaults")
		return cfg, defaultPollIntervals()
	}
	return cfg, cfg.PollIntervals
}

func defaultPollIntervals() contract.PollIntervals {
	return contract.PollIntervals{
		ConfigurationSeconds: 60,
		UsersSeconds:         60,
		TrafficSeconds:       60,
		HeartbeatSeconds:     60,
	}
}

func managedInboundsFromState(curState *state.State) []contract.ManagedInbound {
	managedInbounds := make([]contract.ManagedInbound, 0, len(curState.Inbounds))
	for _, ibState := range curState.Inbounds {
		managedInbounds = append(managedInbounds, contract.ManagedInbound{
			InboundID:       ibState.InboundID,
			Tag:             ibState.Tag,
			Protocol:        ibState.Protocol,
			UserApplyPolicy: contract.ApplyOnUserHotReloadUsers,
		})
	}
	return managedInbounds
}
