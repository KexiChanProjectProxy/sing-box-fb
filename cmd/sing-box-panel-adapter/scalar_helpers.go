package main

import (
	"context"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/config"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/runtime"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

type tokenRotationSchedule struct {
	Path          string
	Interval      time.Duration
	RetryInterval time.Duration
	Logger        log.ContextLogger
}

func runTokenRotation(ctx context.Context, panelClient *client.Client, schedule tokenRotationSchedule) {
	if schedule.Interval <= 0 {
		schedule.Interval = defaultTokenRotationInterval
	}
	if schedule.RetryInterval <= 0 {
		schedule.RetryInterval = tokenRotationRetryInterval
	}
	next := schedule.Interval
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
					schedule.Logger.Warn("token-rotation: ", err)
					next = schedule.RetryInterval
					timer.Reset(next)
					continue
				}
				pending = rotated
			}
			if err := config.UpdateToken(schedule.Path, pending.Token); err != nil {
				schedule.Logger.Warn("token-rotation persist config: ", err)
				next = schedule.RetryInterval
			} else if err := panelClient.SetToken(pending.Token); err != nil {
				schedule.Logger.Warn("token-rotation activate token: ", err)
				next = schedule.RetryInterval
			} else {
				next = schedule.Interval
				if pending.RotateAfterSeconds > 0 {
					next = time.Duration(pending.RotateAfterSeconds) * time.Second
				}
				schedule.Logger.Info("adapter token rotated, expires_at=", pending.ExpiresAt.UTC().Format(time.RFC3339))
				pending = nil
			}
			timer.Reset(next)
		}
	}
}

type periodicTask struct {
	Name     string
	Interval time.Duration
	Logger   log.ContextLogger
	Call     func(context.Context) error
}

func runPeriodic(ctx context.Context, task periodicTask) {
	if task.Interval <= 0 {
		task.Interval = 30 * time.Second
	}
	ticker := time.NewTicker(task.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := task.Call(ctx); err != nil {
				task.Logger.Warn(task.Name, ": ", err)
			}
		}
	}
}

type heartbeatSender struct {
	client  *client.Client
	store   *state.Store
	tracker *traffic.Tracker
	manager *runtime.Manager
	logger  log.ContextLogger
}

func (sender heartbeatSender) Send(ctx context.Context) error {
	current := sender.store.State()
	inbounds := make([]contract.HeartbeatInbound, 0, len(current.Inbounds))
	for _, inbound := range current.Inbounds {
		status := contract.UserLoadStatus(inbound.UserLoadStatus)
		tag := inbound.Tag
		if tag == "" {
			tag = inbound.InboundID
		}
		if status == "" {
			status = contract.UserLoadStatusEmptyInitialLoad
		}
		if !contract.IsSupportedProtocol(inbound.Protocol) {
			status = contract.UserLoadStatusUnsupportedProto
		}
		inbounds = append(inbounds, contract.HeartbeatInbound{
			Tag: tag, Protocol: inbound.Protocol, Status: status, CurrentUserCount: inbound.UserCount,
		})
	}
	runtimeMetrics := sender.tracker.GetRuntimeMetrics()
	runtimeMetrics.UptimeSeconds = int64(time.Since(sender.manager.StartTime()).Seconds())
	heartbeat := &contract.Heartbeat{
		ObservedAt:                   time.Now().UTC(),
		SingBoxVersion:               C.Version,
		AdapterVersion:               C.Version,
		AppliedConfigurationRevision: current.Config.Revision,
		InboundStatuses:              inbounds,
		Runtime:                      runtimeMetrics,
	}
	if err := sender.client.SendHeartbeat(ctx, heartbeat); err != nil {
		return E.Cause(err, "send heartbeat")
	}
	sender.logger.Debug("heartbeat sent, revision=", current.Config.Revision)
	return nil
}

func getStartupConfiguration(panelClient *client.Client, ctx context.Context, logger log.ContextLogger) (*contract.ConfigurationResponse, contract.PollIntervals) {
	configuration, _, err := panelClient.FetchConfiguration(ctx, "")
	if err != nil {
		logger.Warn("fetch poll intervals failed, using defaults: ", err)
		return &contract.ConfigurationResponse{}, defaultPollIntervals()
	}
	intervals := configuration.PollIntervals
	if intervals.ConfigurationSeconds <= 0 || intervals.UsersSeconds <= 0 || intervals.TrafficSeconds <= 0 || intervals.HeartbeatSeconds <= 0 {
		logger.Warn("panel returned invalid poll intervals, using defaults")
		return configuration, defaultPollIntervals()
	}
	return configuration, intervals
}

func defaultPollIntervals() contract.PollIntervals {
	return contract.PollIntervals{
		ConfigurationSeconds: 60,
		UsersSeconds:         60,
		TrafficSeconds:       60,
		HeartbeatSeconds:     60,
	}
}

func managedInboundsFromState(current *state.State) []contract.ManagedInbound {
	managedInbounds := make([]contract.ManagedInbound, 0, len(current.Inbounds))
	for _, inbound := range current.Inbounds {
		managedInbounds = append(managedInbounds, contract.ManagedInbound{
			InboundID: inbound.InboundID, Tag: inbound.Tag, Protocol: inbound.Protocol,
			UserApplyPolicy: contract.ApplyOnUserHotReloadUsers,
		})
	}
	return managedInbounds
}
