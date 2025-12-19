package fsm

import (
	"io"

	"github.com/hashicorp/raft"
)

// FSMSnapshotStore intercepts snapshot creation to fix the index.
//
// Unijord has a two-tier architecture: Raft applies commands to the FSM,
// but a segment is only truly "processed" when it's uploaded to blob storage.
// Applied index != processed index. There's a gap between "Raft applied it"
// and "data is safe in blob".
//
// Raft snapshots at applied index, assuming that's the completion point.
// For us, it creates a false sense that data is done. When a new node joins
// or an existing node restores, it would skip logs that are applied but not
// yet uploaded.
//
// We intercept snapshot creation and set the index to where we've actually
// finished (safe-to-GC index). This ensures both existing and new nodes
// see the correct boundary and don't miss unprocessed segments.
type FSMSnapshotStore struct {
	inner  raft.SnapshotStore
	holder *SafeGCIndexHolder
}

// NewFSMSnapshotStore creates a new FSMSnapshotStore wrapping the given SnapshotStore.
func NewFSMSnapshotStore(inner raft.SnapshotStore, holder *SafeGCIndexHolder) *FSMSnapshotStore {
	return &FSMSnapshotStore{
		inner:  inner,
		holder: holder,
	}
}

// Create creates a new snapshot using the safe-to-GC index/term from the holder
// instead of the applied index/term passed by Raft.
func (s *FSMSnapshotStore) Create(
	version raft.SnapshotVersion,
	index, term uint64,
	configuration raft.Configuration,
	configurationIndex uint64,
	trans raft.Transport,
) (raft.SnapshotSink, error) {
	safeIndex, safeTerm := s.holder.Get()
	// (safeIndex should always be <= appliedIndex)
	if safeIndex > 0 && safeIndex <= index {
		index = safeIndex
		term = safeTerm
	}

	return s.inner.Create(version, index, term, configuration, configurationIndex, trans)
}

// List returns the available snapshots.
func (s *FSMSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	return s.inner.List()
}

// Open opens a snapshot for reading.
func (s *FSMSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	return s.inner.Open(id)
}

var _ raft.SnapshotStore = (*FSMSnapshotStore)(nil)
