package state

type Repository interface {
	State() *State
	UpdateAndSave(update func(*State)) error
}

type NodeStore struct {
	parent *Store
	nodeID string
}

func NewNodeStore(parent *Store, nodeID string) *NodeStore {
	return &NodeStore{parent: parent, nodeID: nodeID}
}

func (store *NodeStore) State() *State {
	root := store.parent.State()
	node, exists := root.Nodes[store.nodeID]
	if !exists {
		node = NodeState{
			Config:   ConfigState{NodeID: store.nodeID},
			Inbounds: make(map[string]InboundState),
		}
	}
	return stateFromNode(root.Version, node)
}

func (store *NodeStore) UpdateAndSave(update func(*State)) error {
	return store.parent.UpdateAndSave(func(root *State) {
		node, exists := root.Nodes[store.nodeID]
		if !exists {
			node = NodeState{
				Config:   ConfigState{NodeID: store.nodeID},
				Inbounds: make(map[string]InboundState),
			}
		}
		child := stateFromNode(root.Version, node)
		update(child)
		root.Nodes[store.nodeID] = NodeState{
			Manifest:       node.Manifest,
			Config:         child.Config,
			Inbounds:       child.Inbounds,
			PendingReports: child.PendingReports,
		}
	})
}

func stateFromNode(version int, node NodeState) *State {
	child := &State{
		Version:        version,
		Config:         node.Config,
		Inbounds:       make(map[string]InboundState, len(node.Inbounds)),
		PendingReports: append([]PendingReport(nil), node.PendingReports...),
	}
	for inboundID, inbound := range node.Inbounds {
		child.Inbounds[inboundID] = inbound
	}
	return child
}

var _ Repository = (*Store)(nil)
var _ Repository = (*NodeStore)(nil)
