package anytls_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"

	anytls "github.com/anytls/sing-anytls"
	"github.com/stretchr/testify/require"
)

const testInboundTag = "anytls-in"

func freePort(t *testing.T) uint16 {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return uint16(listener.Addr().(*net.TCPAddr).Port)
}

func startEchoServer(t *testing.T) M.Socksaddr {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return M.SocksaddrFromNet(listener.Addr())
}

// startBox returns the box and how to stop it, in the order the panel adapter
// does when it replaces one: cancel the context, then close.
func startBox(t *testing.T, port uint16) (*box.Box, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(include.Context(context.Background()))
	options, err := option.UnmarshalContext(ctx, []byte(fmt.Sprintf(`{
		"log": {"disabled": true},
		"inbounds": [{
			"type": "anytls",
			"tag": %q,
			"listen": "127.0.0.1",
			"listen_port": %d,
			"users": [{"name": "u1", "password": "p1"}, {"name": "u2", "password": "p2"}]
		}]
	}`, testInboundTag, port)))
	require.NoError(t, err)
	instance, err := box.New(box.Options{Context: ctx, Options: options})
	require.NoError(t, err)
	require.NoError(t, instance.Start())
	var stopped atomic.Bool
	stop := func() {
		if stopped.CompareAndSwap(false, true) {
			cancel()
			require.NoError(t, instance.Close())
		}
	}
	t.Cleanup(stop)
	return instance, stop
}

// newClient returns a client and the number of sessions it has dialed.
func newClient(t *testing.T, port uint16, password string) (*anytls.Client, *atomic.Int32) {
	t.Helper()
	var dials atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client, err := anytls.NewClient(ctx, anytls.ClientConfig{
		Password: password,
		DialOut: func(ctx context.Context) (net.Conn, error) {
			dials.Add(1)
			var dialer net.Dialer
			return dialer.DialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", port))
		},
		Logger: logger.NOP(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return client, &dials
}

func requireEcho(t *testing.T, conn net.Conn) {
	t.Helper()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	_, err := conn.Write([]byte("ping"))
	require.NoError(t, err)
	reply := make([]byte, 4)
	_, err = io.ReadFull(conn, reply)
	require.NoError(t, err)
	require.Equal(t, "ping", string(reply))
}

// poolSession leaves one idle session in the client's pool.
func poolSession(t *testing.T, client *anytls.Client, destination M.Socksaddr) {
	t.Helper()
	conn, err := client.CreateProxy(context.Background(), destination)
	require.NoError(t, err)
	requireEcho(t, conn)
	require.NoError(t, conn.Close())
}

// A session the client pooled before the box was replaced must not be handed
// out afterwards: it would run every stream on the closed box.
func TestRecreateDropsPooledSession(t *testing.T) {
	destination := startEchoServer(t)
	port := freePort(t)
	_, stopOldBox := startBox(t, port)
	client, dials := newClient(t, port, "p1")
	poolSession(t, client, destination)
	require.EqualValues(t, 1, dials.Load())

	stopOldBox()
	startBox(t, port)
	// What a client sees of the close takes a moment to arrive.
	time.Sleep(200 * time.Millisecond)

	conn, err := client.CreateProxy(context.Background(), destination)
	require.NoError(t, err)
	defer conn.Close()
	requireEcho(t, conn)
	require.EqualValues(t, 2, dials.Load(), "stream reused a session of the closed box")
}

func TestReplaceUsersDropsRemovedUserOnly(t *testing.T) {
	destination := startEchoServer(t)
	port := freePort(t)
	instance, _ := startBox(t, port)

	kept, _ := newClient(t, port, "p1")
	removed, _ := newClient(t, port, "p2")
	keptConn, err := kept.CreateProxy(context.Background(), destination)
	require.NoError(t, err)
	defer keptConn.Close()
	requireEcho(t, keptConn)
	removedConn, err := removed.CreateProxy(context.Background(), destination)
	require.NoError(t, err)
	defer removedConn.Close()
	requireEcho(t, removedConn)

	require.NoError(t, instance.ReplaceInboundUsers(testInboundTag, []adapter.ManagedUser{{
		UserID:     "u1",
		Credential: adapter.ManagedUserCredential{Password: "p1"},
	}}))

	// The connection may already be closed, which fails this too.
	_ = removedConn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = removedConn.Read(make([]byte, 1))
	require.Error(t, err)
	var netErr net.Error
	if errors.As(err, &netErr) {
		require.False(t, netErr.Timeout(), "connection of a removed user stayed open")
	}
	requireEcho(t, keptConn)
}
