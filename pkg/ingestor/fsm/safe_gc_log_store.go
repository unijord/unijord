package fsm

import (
	"github.com/hashicorp/raft"
)

// SafeGCLogStore wraps a LogStore to prevent premature log deletion.
//
// Raft by default wants to delete logs after snapshotting, but we store
// events in those logs. A segment that's SEALED but not yet UPLOADED still
// needs its log entries for processing.
//
// Raft calls DeleteRange(1, 400) after applying index 400. But if only
// segments up to index 200 are uploaded, we'd lose data for indices 201-400.
//
// This wrapper intercepts DeleteRange and caps the max to our safe-to-GC
// index. Raft asks to delete up to 400, we delete up to 200 instead.
type SafeGCLogStore struct {
	inner  raft.LogStore
	holder *SafeGCIndexHolder
}

// NewSafeGCLogStore creates a new SafeGCLogStore wrapping the given LogStore.
func NewSafeGCLogStore(inner raft.LogStore, holder *SafeGCIndexHolder) *SafeGCLogStore {
	return &SafeGCLogStore{
		inner:  inner,
		holder: holder,
	}
}

// FirstIndex returns the first index written. 0 for no entries.
func (s *SafeGCLogStore) FirstIndex() (uint64, error) {
	return s.inner.FirstIndex()
}

// LastIndex returns the last index written. 0 for no entries.
func (s *SafeGCLogStore) LastIndex() (uint64, error) {
	return s.inner.LastIndex()
}

// GetLog gets a log entry at a given index.
func (s *SafeGCLogStore) GetLog(index uint64, log *raft.Log) error {
	return s.inner.GetLog(index, log)
}

// StoreLog stores a log entry.
func (s *SafeGCLogStore) StoreLog(log *raft.Log) error {
	return s.inner.StoreLog(log)
}

// StoreLogs stores multiple log entries.
func (s *SafeGCLogStore) StoreLogs(logs []*raft.Log) error {
	return s.inner.StoreLogs(logs)
}

// DeleteRange deletes a range of log entries, but limits the max to the
// safe-to-GC index to preserve logs needed for pending segment processing.
func (s *SafeGCLogStore) DeleteRange(min, max uint64) error {
	safeIndex, _ := s.holder.Get()

	if safeIndex > 0 && safeIndex < max {
		max = safeIndex
	}

	if min > max {
		return nil
	}

	return s.inner.DeleteRange(min, max)
}

var _ raft.LogStore = (*SafeGCLogStore)(nil)
