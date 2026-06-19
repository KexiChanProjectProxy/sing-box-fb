package main

import (
	"context"
	"testing"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

func TestReplaceInboundUsersNotFound(t *testing.T) {
	ctx := include.Context(context.Background())
	instance, err := box.New(box.Options{
		Context: ctx,
		Options: option.Options{
			Log: &option.LogOptions{Level: "warning"},
			Inbounds: []option.Inbound{
				{
					Type: "direct",
					Tag:  "direct-in",
				},
			},
			Outbounds: []option.Outbound{
				{
					Type: "direct",
					Tag:  "direct-out",
				},
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { instance.Close() })

	err = instance.ReplaceInboundUsers("nonexistent-tag", nil)
	require.ErrorIs(t, err, adapter.ErrInboundNotFound)
}

func TestReplaceInboundUsersNotManaged(t *testing.T) {
	ctx := include.Context(context.Background())
	instance, err := box.New(box.Options{
		Context: ctx,
		Options: option.Options{
			Log: &option.LogOptions{Level: "warning"},
			Inbounds: []option.Inbound{
				{
					Type: "direct",
					Tag:  "direct-in",
				},
			},
			Outbounds: []option.Outbound{
				{
					Type: "direct",
					Tag:  "direct-out",
				},
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { instance.Close() })

	err = instance.ReplaceInboundUsers("direct-in", nil)
	require.ErrorIs(t, err, adapter.ErrInboundNotManaged)
}
