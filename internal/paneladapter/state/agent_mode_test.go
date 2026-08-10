package state

import (
	"path/filepath"
	"testing"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
)

func TestAgentMode_RestartRecovery(t *testing.T) {
	// Given
	path := filepath.Join(t.TempDir(), "state.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	manifest := contract.AgentManifest{
		APIVersion:       contract.AgentManifestAPIVersion,
		AgentID:          "0190f4d8-76d8-7a5c-9ea7-f4e7a89f78b1",
		ManifestRevision: 9,
		Reconciliation:   contract.AgentManifestReconciliation{FullSnapshot: true, PollAfterSeconds: 30},
		Nodes: []contract.AgentManifestNode{
			{NodeID: "0190f4d8-76d8-7a5c-9ea7-f4e7a89f78b2", Status: contract.AgentManifestNodeStatusActive, AssignmentRevision: 2, ConfigurationResource: "/api/v1/nodes/0190f4d8-76d8-7a5c-9ea7-f4e7a89f78b2/configuration", UserResource: "/api/v1/nodes/0190f4d8-76d8-7a5c-9ea7-f4e7a89f78b2/inbounds/0190f4d8-76d8-7a5c-9ea7-f4e7a89f78b2/users"},
		},
	}
	if err := store.UpdateAndSave(func(st *State) {
		st.Manifest = ManifestState{ETag: `"manifest-9"`, Snapshot: manifest}
		st.Nodes[manifest.Nodes[0].NodeID] = NodeState{Config: ConfigState{NodeID: manifest.Nodes[0].NodeID, Revision: "cfg-4"}, Inbounds: map[string]InboundState{}}
	}); err != nil {
		t.Fatal(err)
	}

	// When
	reloaded, err := NewStore(path)

	// Then
	if err != nil {
		t.Fatal(err)
	}
	state := reloaded.State()
	if state.Manifest.ETag != `"manifest-9"` || state.Manifest.Snapshot.ManifestRevision != 9 {
		t.Fatalf("manifest state = %#v", state.Manifest)
	}
	if state.Nodes[manifest.Nodes[0].NodeID].Config.Revision != "cfg-4" {
		t.Fatalf("node state = %#v", state.Nodes)
	}
}

func TestAgentMode_NodeStateIsolation(t *testing.T) {
	// Given
	store, err := NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	one := NewNodeStore(store, "node-one")
	two := NewNodeStore(store, "node-two")

	// When
	if err := one.UpdateAndSave(func(st *State) {
		st.Config.Revision = "cfg-one"
	}); err != nil {
		t.Fatal(err)
	}
	if err := two.UpdateAndSave(func(st *State) {
		st.Config.Revision = "cfg-two"
	}); err != nil {
		t.Fatal(err)
	}

	// Then
	if one.State().Config.Revision != "cfg-one" || two.State().Config.Revision != "cfg-two" {
		t.Fatalf("node revisions = %q/%q", one.State().Config.Revision, two.State().Config.Revision)
	}
}
