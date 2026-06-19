package contract

import (
	"embed"
	"strings"
	"testing"
)

//go:embed *.go
var contractSource embed.FS

// forbiddenPatterns lists strings that must never appear in adapter source code.
// These patterns indicate accidental compatibility with V2bX, V2Board, XrayR,
// or SSPanel APIs that the adapter explicitly does not support.
var forbiddenPatterns = []struct {
	pattern string
	reason  string
}{
	{"UniProxy", "V2bX compatibility endpoint"},
	{"/server/v1", "V2bX server API path"},
	{"/api/v1/server/", "V2bX server API path prefix"},
	{"uPSK", "V2bX Shadowsocks multi-user key format"},
	{"node_type", "V2bX node type field"},
	{"ApiKey", "V2bX configuration field name"},
	{"V2Board", "legacy panel name"},
	{"XrayR", "legacy panel agent name"},
	{"SSPanel", "legacy panel name"},
}

// excludedFiles lists filenames where forbidden patterns are allowed to appear
// (e.g., this test file itself mentions the patterns in its detection logic).
var excludedFiles = map[string]bool{
	"compat_test.go": true,
}

// TestForbiddenCompatibilityStrings scans all .go files in the contract package
// for forbidden compatibility patterns. This test runs as part of the adapter
// test suite to prevent accidental introduction of V2bX/V2Board/XrayR/SSPanel
// compatibility code.
func TestForbiddenCompatibilityStrings(t *testing.T) {
	entries, err := contractSource.ReadDir(".")
	if err != nil {
		t.Fatalf("read embedded directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		if excludedFiles[entry.Name()] {
			continue
		}

		data, err := contractSource.ReadFile(entry.Name())
		if err != nil {
			t.Fatalf("read embedded file %s: %v", entry.Name(), err)
		}
		content := string(data)

		for _, fp := range forbiddenPatterns {
			if strings.Contains(content, fp.pattern) {
				t.Errorf("FORBIDDEN: file %q contains %q (%s) — this adapter must not implement V2bX/V2Board/XrayR/SSPanel compatibility",
					entry.Name(), fp.pattern, fp.reason)
			}
		}
	}
}
