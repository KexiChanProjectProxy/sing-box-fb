package main

import (
	"context"
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
)

func runScalarAdapter(configuration *config.Config) error {
	logLevel := configuration.LogLevel
	if logLevel == "" {
		logLevel = "info"
	}
	logFactory, err := log.New(log.Options{
		Context: context.Background(),
		Options: option.LogOptions{Level: logLevel, Output: "stderr", Timestamp: true},
	})
	if err != nil {
		return E.Cause(err, "create log factory")
	}
	if err := logFactory.Start(); err != nil {
		return E.Cause(err, "start log factory")
	}
	defer func() { _ = logFactory.Close() }()
	logger := logFactory.NewLogger("panel/adapter")
	ctx := include.Context(context.Background())
	panelClient, err := client.New(
		configuration.PanelBaseURL,
		configuration.NodeID,
		configuration.NodeToken,
		client.WithLogger(logger),
	)
	if err != nil {
		return E.Cause(err, "create panel client")
	}
	logger.Info("panel client created for node_id=", configuration.NodeID)
	store, err := state.NewStore(configuration.StatePath)
	if err != nil {
		return E.Cause(err, "create state store")
	}
	tracker := traffic.NewTracker(nil)
	manager, err := runtime.NewManager(panelClient, store, tracker, logFactory)
	if err != nil {
		return E.Cause(err, "create runtime manager")
	}
	if err := manager.Bootstrap(ctx); err != nil {
		return E.Cause(err, "bootstrap")
	}
	current := store.State()
	updateScalarTrackerMapping(tracker, current)
	logger.Info("bootstrap complete, revision=", current.Config.Revision)
	startupConfiguration, pollIntervals := getStartupConfiguration(panelClient, ctx, logger)
	poller := users.NewPoller(panelClient, store, manager.GetBox(), logger)
	report := reporter.NewReporter(
		panelClient,
		store,
		tracker,
		configuration.NodeID,
		reporter.WithLogger(logger),
		reporter.WithConfigRevision(current.Config.Revision),
	)
	if err := report.Recovery(ctx); err != nil {
		logger.Warn("reporter recovery: ", err)
	}
	adapterContext, cancel := context.WithCancel(ctx)
	defer cancel()
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		runPeriodic(adapterContext, periodicTask{Name: "config-poll", Interval: time.Duration(pollIntervals.ConfigurationSeconds) * time.Second, Logger: logger, Call: func(ctx context.Context) error {
			if err := manager.PollConfiguration(ctx); err != nil {
				return E.Cause(err, "poll configuration")
			}
			stateSnapshot := store.State()
			updateScalarTrackerMapping(tracker, stateSnapshot)
			report.SetConfigRevision(stateSnapshot.Config.Revision)
			return nil
		}})
	}()
	managedInbounds := startupConfiguration.ManagedInbounds
	if len(managedInbounds) == 0 {
		managedInbounds = managedInboundsFromState(current)
	}
	for _, managedInbound := range managedInbounds {
		if !contract.IsSupportedProtocol(managedInbound.Protocol) {
			continue
		}
		inboundID := managedInbound.InboundID
		waitGroup.Add(1)
		go func(inboundID string, inbound contract.ManagedInbound) {
			defer waitGroup.Done()
			runPeriodic(adapterContext, periodicTask{Name: "user-poll/" + inboundID, Interval: time.Duration(pollIntervals.UsersSeconds) * time.Second, Logger: logger, Call: func(ctx context.Context) error {
				return poller.PollInbound(ctx, inboundID, inbound, store.State().Config.Revision)
			}})
		}(inboundID, managedInbound)
	}
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		runPeriodic(adapterContext, periodicTask{Name: "traffic-report", Interval: time.Duration(pollIntervals.TrafficSeconds) * time.Second, Logger: logger, Call: report.ReportNow})
	}()
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		sender := heartbeatSender{client: panelClient, store: store, tracker: tracker, manager: manager, logger: logger}
		runPeriodic(adapterContext, periodicTask{Name: "heartbeat", Interval: time.Duration(pollIntervals.HeartbeatSeconds) * time.Second, Logger: logger, Call: sender.Send})
	}()
	rotationInterval := configuration.TokenRotationInterval.Duration
	if rotationInterval <= 0 {
		rotationInterval = defaultTokenRotationInterval
	}
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		runTokenRotation(adapterContext, panelClient, tokenRotationSchedule{Path: configPath, Interval: rotationInterval, Logger: logger})
	}()
	logger.Info("adapter running — all goroutines started")
	reloader := scalarConfigReloader{current: configuration, client: panelClient, logFactory: logFactory, logger: logger}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(signals)
	for {
		received := <-signals
		if received == syscall.SIGHUP {
			logger.Info("received SIGHUP — re-reading local adapter config")
			reloader.Reload()
			continue
		}
		logger.Info("received ", received, " — shutting down")
		cancel()
		break
	}
	waitGroup.Wait()
	if err := store.Save(); err != nil {
		logger.Warn("flush state on shutdown: ", err)
	}
	if err := manager.Close(); err != nil {
		logger.Warn("close box: ", err)
	}
	go func() {
		time.Sleep(C.FatalStopTimeout)
		os.Exit(1)
	}()
	debug.FreeOSMemory()
	logger.Info("adapter stopped")
	return nil
}

type scalarConfigReloader struct {
	current    *config.Config
	client     *client.Client
	logFactory log.Factory
	logger     log.ContextLogger
}

func (reloader scalarConfigReloader) Reload() {
	loaded, err := config.Load(configPath)
	if err != nil {
		reloader.logger.Warn("reload adapter config: ", err)
		return
	}
	levelName := loaded.LogLevel
	if levelName == "" {
		levelName = "info"
	}
	level, err := log.ParseLevel(levelName)
	if err != nil {
		reloader.logger.Warn("parse log level: ", err)
	} else {
		reloader.logFactory.SetLevel(level)
	}
	*reloader.current = *loaded
	if err := reloader.client.SetToken(loaded.BearerToken()); err != nil {
		reloader.logger.Warn("reload adapter token: ", err)
	}
	reloader.logger.Info("adapter config reloaded")
}

func updateScalarTrackerMapping(tracker *traffic.Tracker, current *state.State) {
	mapping := make(map[string]string, len(current.Inbounds))
	for _, inbound := range current.Inbounds {
		mapping[inbound.Tag] = inbound.InboundID
	}
	tracker.UpdateInboundMapping(mapping)
}
