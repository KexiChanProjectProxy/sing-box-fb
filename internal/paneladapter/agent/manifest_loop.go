package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sagernet/sing-box/internal/paneladapter/client"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
)

func (controller *Controller) Run(ctx context.Context) error {
	if controller.fetcher == nil {
		return fmt.Errorf("agent manifest fetcher is required")
	}
	defer controller.Close()
	if err := controller.Restore(ctx); err != nil {
		return fmt.Errorf("restore agent state: %w", err)
	}
	retryDelay := time.Duration(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		current := controller.store.State().Manifest
		manifest, etag, err := controller.fetcher.FetchManifest(ctx, current.ETag)
		if errors.Is(err, client.ErrNotModified) {
			retryDelay = 0
			if err := controller.wait(ctx, manifestPollDelay(current.Snapshot)); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			if !client.IsRetryable(err) {
				return fmt.Errorf("fetch agent manifest: %w", err)
			}
			retryDelay = controller.retry.Next(retryDelay)
			if err := controller.wait(ctx, retryDelay); err != nil {
				return err
			}
			continue
		}
		if err := controller.Reconcile(ctx, manifest); err != nil {
			retryDelay = controller.retry.Next(retryDelay)
			if err := controller.wait(ctx, retryDelay); err != nil {
				return err
			}
			continue
		}
		if err := controller.store.UpdateAndSave(func(root *state.State) {
			root.Manifest.ETag = etag
		}); err != nil {
			return fmt.Errorf("persist agent manifest etag: %w", err)
		}
		retryDelay = 0
		if err := controller.wait(ctx, manifestPollDelay(*manifest)); err != nil {
			return err
		}
	}
}

func (controller *Controller) Restore(ctx context.Context) error {
	manifest := controller.store.State().Manifest.Snapshot
	if manifest.APIVersion == "" {
		return nil
	}
	return controller.Reconcile(ctx, &manifest)
}

func manifestPollDelay(manifest contract.AgentManifest) time.Duration {
	seconds := manifest.Reconciliation.PollAfterSeconds
	if seconds <= 0 {
		seconds = 30
	}
	return time.Duration(seconds) * time.Second
}

func waitFor(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
