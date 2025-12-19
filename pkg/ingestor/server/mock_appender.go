package server

import "sync/atomic"

// MockAppender is a test implementation of Appender.
type MockAppender struct {
	isLeader   atomic.Bool
	leaderAddr atomic.Value
	lsn        atomic.Uint64
	entries    [][]byte
	appendErr  error

	stats AppenderStats
}

// NewMockAppender creates a mock appender for testing.
func NewMockAppender() *MockAppender {
	m := &MockAppender{}
	m.isLeader.Store(true)
	m.leaderAddr.Store("")
	m.lsn.Store(0)
	return m
}

func (m *MockAppender) AppendBatch(entries [][]byte) (uint64, error) {
	if m.appendErr != nil {
		return 0, m.appendErr
	}
	if !m.isLeader.Load() {
		return 0, ErrNotLeader
	}
	m.entries = append(m.entries, entries...)
	return m.lsn.Add(1), nil
}

func (m *MockAppender) IsLeader() bool {
	return m.isLeader.Load()
}

func (m *MockAppender) LeaderAddr() string {
	v := m.leaderAddr.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

func (m *MockAppender) Stats() AppenderStats {
	return m.stats
}

func (m *MockAppender) SetLeader(isLeader bool) {
	m.isLeader.Store(isLeader)
}

func (m *MockAppender) SetLeaderAddr(addr string) {
	m.leaderAddr.Store(addr)
}

func (m *MockAppender) SetAppendError(err error) {
	m.appendErr = err
}

func (m *MockAppender) SetStats(stats AppenderStats) {
	m.stats = stats
}

func (m *MockAppender) GetEntries() [][]byte {
	return m.entries
}

func (m *MockAppender) Reset() {
	m.isLeader.Store(true)
	m.leaderAddr.Store("")
	m.lsn.Store(0)
	m.entries = nil
	m.appendErr = nil
	m.stats = AppenderStats{}
}

var _ Appender = (*MockAppender)(nil)
