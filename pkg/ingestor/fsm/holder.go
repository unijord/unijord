package fsm

import (
	"sync/atomic"
)

// SafeGCIndexHolder shares the safe-to-GC index between FSM and snapshot store.
//
// FSM updates this when segments are uploaded. FSMSnapshotStore reads it when
// creating snapshots. The holder is the bridge between them.
//
// Example: We have 4 segments. Segments 1-2 are uploaded, 3-4 are still sealed.
// Raft has applied through index 400, but only indices 1-200 are truly safe
// (backed by uploaded segments). The holder stores 200, not 400.
//
// When Raft snapshots, FSMSnapshotStore reads 200 from the holder and uses
// that instead of 400. Logs 201-400 survive for segments 3-4.
type SafeGCIndexHolder struct {
	index atomic.Uint64
	term  atomic.Uint64
}

// NewSafeGCIndexHolder creates a new SafeGCIndexHolder.
func NewSafeGCIndexHolder() *SafeGCIndexHolder {
	return &SafeGCIndexHolder{}
}

// Set stores the safe-to-GC index and term.
// Mostly should be called by the SEGMENT_UPLOADED.
func (h *SafeGCIndexHolder) Set(index, term uint64) {
	// advance, never go backwards
	current := h.index.Load()
	if index > current {
		h.index.Store(index)
		h.term.Store(term)
	}
}

// Get retrieves the safe-to-GC index and term.
func (h *SafeGCIndexHolder) Get() (index, term uint64) {
	return h.index.Load(), h.term.Load()
}

// Index returns just the safe-to-GC index.
func (h *SafeGCIndexHolder) Index() uint64 {
	return h.index.Load()
}

// Term returns just the safe-to-GC term.
func (h *SafeGCIndexHolder) Term() uint64 {
	return h.term.Load()
}
