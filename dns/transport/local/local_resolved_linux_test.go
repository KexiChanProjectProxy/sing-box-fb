package local

import (
	"bufio"
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"

	"github.com/godbus/dbus/v5"
)

// A copied resolv.conf can still mention systemd-resolved after its service has
// been removed. Initialization must fail so Transport keeps using resolv.conf.
func TestResolvedStartRejectsUnavailableService(t *testing.T) {
	if _, err := exec.LookPath("dbus-daemon"); err != nil {
		t.Skip("dbus-daemon is required for an isolated bus")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "dbus-daemon", "--session", "--nofork", "--nopidfile", "--print-address=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("read isolated bus address: %v", scanner.Err())
	}
	conn, err := dbus.Connect(scanner.Text())
	if err != nil {
		t.Fatal(err)
	}
	resolver := &DBusResolvedResolver{ctx: ctx, logger: log.NewNOPFactory().Logger(), systemBus: conn}
	t.Cleanup(func() { _ = resolver.Close() })
	if err = resolver.Start(); err == nil {
		t.Fatal("unavailable systemd-resolved must not replace resolv.conf DNS")
	}
	if resolver.savedServerSet.Load() != nil {
		t.Fatal("unavailable resolver installed a server set")
	}
}
