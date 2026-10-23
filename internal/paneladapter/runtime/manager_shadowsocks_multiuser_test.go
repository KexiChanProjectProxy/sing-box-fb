package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	"github.com/sagernet/sing-box/internal/paneladapter/state"
	"github.com/sagernet/sing-box/internal/paneladapter/traffic"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-shadowsocks/shadowaead_2022"
	M "github.com/sagernet/sing/common/metadata"
)

func TestManagedShadowsocksNetworkAuthenticationAndAccounting(t *testing.T) {
	const method = "2022-blake3-aes-128-gcm"
	const serverKey = "MDEyMzQ1Njc4OWFiY2RlZg=="
	const keyA = "YWJjZGVmZ2hpamtsbW5vcA=="
	const keyB = "cG9ubWxramloZ2ZlZGNiYQ=="
	const userA = "019b2d8a-4b11-7000-8000-000000000001"
	const userB = "019b2d8a-4b11-7000-8000-000000000002"
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, strings.Repeat(r.URL.Path, 64)) }))
	defer target.Close()
	template := fmt.Sprintf(`{"log":{"disabled":true},"inbounds":[{"type":"shadowsocks","tag":"ss-managed","listen":"127.0.0.1","listen_port":%d,"method":%q,"password":%q,"users":[{"name":"stale","password":%q}]}],"outbounds":[{"type":"direct","tag":"direct"}],"route":{"final":"direct"}}`, port, method, serverKey, keyA)
	cfg := &contract.ConfigurationResponse{NodeID: "node-ss", Revision: "ss-multi-v1", SingBoxConfigTemplate: json.RawMessage(template), ManagedInbounds: []contract.ManagedInbound{{InboundID: "node-ss", Tag: "ss-managed", Protocol: contract.ProtocolShadowsocks, UserApplyPolicy: contract.ApplyOnUserHotReloadUsers}}}
	store, err := state.NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	tracker := traffic.NewTracker(map[string]string{"ss-managed": "node-ss"})
	m, err := NewManager(&fakeClient{}, store, tracker, log.NewNOPFactory())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.applyConfigLocked(context.Background(), cfg, "etag-ss"); err != nil {
		t.Fatal(err)
	}
	defer m.instance.Close()
	probe := func(key, path string) error {
		method, err := shadowaead_2022.NewWithPassword(method, serverKey+":"+key, nil)
		if err != nil {
			return err
		}
		raw, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err != nil {
			return err
		}
		defer raw.Close()
		raw.SetDeadline(time.Now().Add(time.Second))
		conn, err := method.DialConn(raw, M.ParseSocksaddr(strings.TrimPrefix(target.URL, "http://")))
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", path); err != nil {
			return err
		}
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		if string(body) != strings.Repeat(path, 64) {
			return fmt.Errorf("unexpected proxy response")
		}
		return nil
	}
	// Stale embedded users must not be accepted before the first snapshot.
	if probe(keyA, "/before") == nil {
		t.Fatal("managed inbound accepted embedded user")
	}
	users := []adapter.ManagedUser{{UserID: userA, Name: "alice", Credential: adapter.ManagedUserCredential{Password: keyA}}, {UserID: userB, Name: "bob", Credential: adapter.ManagedUserCredential{Password: keyB}}}
	if err := m.ReplaceInboundUsers("ss-managed", users); err != nil {
		t.Fatal(err)
	}
	if err := probe(keyA, "/alice"); err != nil {
		t.Fatal(err)
	}
	if err := probe(keyB, "/bob-longer"); err != nil {
		t.Fatal(err)
	}
	// Reorder and rename the snapshot; metering must keep immutable identities.
	users[0], users[1] = users[1], users[0]
	users[0].Name = "renamed-bob"
	if err := m.ReplaceInboundUsers("ss-managed", users); err != nil {
		t.Fatal(err)
	}
	if err := probe(keyB, "/bob-longer"); err != nil {
		t.Fatal(err)
	}
	if err := m.ReplaceInboundUsers("ss-managed", users[:1]); err != nil {
		t.Fatal(err)
	}
	if probe(keyA, "/revoked") == nil {
		t.Fatal("revoked user still authenticates")
	}
	if err := probe(keyB, "/bob-longer"); err != nil {
		t.Fatal(err)
	}
	report := tracker.StageForReport(time.Now().Add(-time.Minute), time.Now(), cfg.Revision)
	if len(report.Records) != 2 {
		t.Fatalf("expected two individually metered users, got %d", len(report.Records))
	}
	seen := map[string]bool{}
	for _, item := range report.Records {
		if item.UserID != userA && item.UserID != userB {
			t.Fatal("traffic attributed to mutable name or position")
		}
		if item.UploadBytes <= 0 || item.DownloadBytes <= 0 {
			t.Fatal("traffic counters empty")
		}
		seen[item.UserID] = true
	}
	if !seen[userA] || !seen[userB] {
		t.Fatal("missing user traffic")
	}
	if err := m.ReplaceInboundUsers("ss-managed", nil); err != nil {
		t.Fatal(err)
	}
	if probe(keyB, "/disabled") == nil {
		t.Fatal("empty snapshot did not revoke all users")
	}
}
