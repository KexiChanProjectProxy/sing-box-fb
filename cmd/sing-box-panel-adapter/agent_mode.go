package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/internal/paneladapter/agent"
	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/config"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

func runAgentAdapter(cfg *config.Config) error {
	logLevel := cfg.LogLevel
	if logLevel == "" {
		logLevel = "info"
	}
	logFactory, err := log.New(log.Options{
		Context: context.Background(),
		Options: option.LogOptions{Level: logLevel, Output: "stderr", Timestamp: true},
	})
	if err != nil {
		return E.Cause(err, "create agent log factory")
	}
	if err := logFactory.Start(); err != nil {
		return E.Cause(err, "start agent log factory")
	}
	defer func() { _ = logFactory.Close() }()
	logger := logFactory.NewLogger("panel/agent")
	rootContext := include.Context(context.Background())
	httpTimeout := cfg.HTTPTimeout.Duration
	if httpTimeout <= 0 {
		httpTimeout = 30 * time.Second
	}
	panelClient, err := client.NewAgent(
		cfg.PanelBaseURL,
		cfg.AgentID,
		cfg.AgentToken,
		client.WithHTTPClient(&http.Client{Timeout: httpTimeout}),
		client.WithLogger(logger),
	)
	if err != nil {
		return E.Cause(err, "create agent panel client")
	}
	store, err := state.NewStore(cfg.StatePath)
	if err != nil {
		return E.Cause(err, "create agent state store")
	}
	sessions, err := agent.NewRuntimeSessionFactory(agent.RuntimeSessionFactoryOptions{
		Client: panelClient, Store: store, LogFactory: logFactory, Logger: logger,
	})
	if err != nil {
		return E.Cause(err, "create agent runtime sessions")
	}
	worker, err := agent.NewNodeWorker(agent.NodeWorkerOptions{
		Sessions: sessions,
		Logger:   logger,
		Intervals: agent.PollIntervalPolicy{
			Minimum: pollBound(cfg, true),
			Maximum: pollBound(cfg, false),
		},
	})
	if err != nil {
		return E.Cause(err, "create agent node worker")
	}
	controller, err := agent.NewController(agent.ControllerOptions{
		Context: rootContext,
		Fetcher: panelClient,
		Store:   store,
		Workers: worker,
		Retry:   agent.RetryPolicy{Minimum: time.Second, Maximum: 30 * time.Second},
	})
	if err != nil {
		return E.Cause(err, "create agent controller")
	}
	adapterContext, cancel := context.WithCancel(rootContext)
	defer cancel()
	var waitGroup sync.WaitGroup
	controllerResult := make(chan error, 1)
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		controllerResult <- controller.Run(adapterContext)
	}()
	rotationInterval := cfg.TokenRotationInterval.Duration
	if rotationInterval <= 0 {
		rotationInterval = defaultTokenRotationInterval
	}
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		runTokenRotation(adapterContext, panelClient, tokenRotationSchedule{Path: configPath, Interval: rotationInterval, Logger: logger})
	}()
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		runAgentHeartbeat(adapterContext, panelClient, store, cfg.AgentID, logger)
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(signals)
	logger.Info("agent adapter running for agent_id=", cfg.AgentID)
	reloader := agentConfigReloader{current: cfg, client: panelClient, logFactory: logFactory, logger: logger}
	var runErr error
	for runErr == nil {
		select {
		case result := <-controllerResult:
			if result != nil && !errors.Is(result, context.Canceled) {
				runErr = fmt.Errorf("agent controller: %w", result)
			} else {
				runErr = context.Canceled
			}
			cancel()
		case received := <-signals:
			if received == syscall.SIGHUP {
				if err := reloader.Reload(); err != nil {
					logger.Warn("reload agent config: ", err)
				}
				continue
			}
			cancel()
			runErr = context.Canceled
		}
	}
	controller.Close()
	waitGroup.Wait()
	if err := store.Save(); err != nil {
		logger.Warn("flush agent state: ", err)
	}
	if errors.Is(runErr, context.Canceled) {
		return nil
	}
	return runErr
}

type agentConfigReloader struct {
	current    *config.Config
	client     *client.Client
	logFactory log.Factory
	logger     log.ContextLogger
}

func (reloader agentConfigReloader) Reload() error {
	loaded, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if !loaded.IsAgentMode() || loaded.AgentID != reloader.current.AgentID {
		return fmt.Errorf("agent identity cannot change during reload")
	}
	if err := reloader.client.SetToken(loaded.AgentToken); err != nil {
		return err
	}
	levelName := loaded.LogLevel
	if levelName == "" {
		levelName = "info"
	}
	level, err := log.ParseLevel(levelName)
	if err != nil {
		return err
	}
	reloader.logFactory.SetLevel(level)
	*reloader.current = *loaded
	reloader.logger.Info("agent adapter config reloaded")
	return nil
}

func pollBound(cfg *config.Config, minimum bool) time.Duration {
	if cfg.PollIntervalBounds == nil {
		if minimum {
			return time.Second
		}
		return 5 * time.Minute
	}
	seconds := cfg.PollIntervalBounds.MaxSeconds
	if minimum {
		seconds = cfg.PollIntervalBounds.MinSeconds
	}
	return time.Duration(seconds) * time.Second
}
