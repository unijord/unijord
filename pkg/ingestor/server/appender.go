package server

import "errors"

var (
	ErrNotLeader    = errors.New("not the leader")
	ErrShuttingDown = errors.New("shutting down")
)

// Appender is the interface for event ingestion.
type Appender interface {
	// AppendBatch appends a batch of WAL entries and returns the committed LSN.
	// All entries in the batch are stored atomically as a single Raft log entry.
	// So care must be taken how much we want to store in single batch.
	AppendBatch(entries [][]byte) (lsn uint64, err error)

	IsLeader() bool
	LeaderAddr() string
	Stats() AppenderStats
}

// AppenderStats contains statistics for status reporting.
type AppenderStats struct {
	NodeID               string
	IsLeader             bool
	LeaderAddr           string
	ActiveSegmentID      uint32
	ActiveSegmentBytes   uint64
	ActiveSegmentEntries uint64
	PendingSegments      int
}

// Health return healthy if this node is leader.
func (s AppenderStats) Health() string {
	if !s.IsLeader && s.LeaderAddr == "" {
		return "degraded"
	}
	return "healthy"
}
