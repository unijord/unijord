package fsm

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"

	"github.com/unijord/unijord/pkg/ingestor/command"
)

type mockRaftApplier struct {
	mu       sync.Mutex
	state    raft.RaftState
	commands [][]byte
	applyErr error
	servers  []raft.Server
}

func newMockRaftApplier(state raft.RaftState) *mockRaftApplier {
	return &mockRaftApplier{
		state: state,
		servers: []raft.Server{
			{ID: "node-0", Suffrage: raft.Voter},
			{ID: "node-1", Suffrage: raft.Voter},
			{ID: "node-2", Suffrage: raft.Voter},
		},
	}
}

func (m *mockRaftApplier) Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commands = append(m.commands, cmd)
	return &mockApplyFuture{err: m.applyErr}
}

func (m *mockRaftApplier) State() raft.RaftState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

func (m *mockRaftApplier) SetState(state raft.RaftState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
}

func (m *mockRaftApplier) GetCommands() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([][]byte, len(m.commands))
	copy(result, m.commands)
	return result
}

func (m *mockRaftApplier) GetConfiguration() raft.ConfigurationFuture {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &mockConfigurationFuture{
		config: raft.Configuration{Servers: m.servers},
	}
}

func (m *mockRaftApplier) SetServers(servers []raft.Server) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.servers = servers
}

type mockConfigurationFuture struct {
	config raft.Configuration
	err    error
}

func (m *mockConfigurationFuture) Error() error {
	return m.err
}

func (m *mockConfigurationFuture) Configuration() raft.Configuration {
	return m.config
}

func (m *mockConfigurationFuture) Index() uint64 {
	return 0
}

type mockApplyFuture struct {
	err error
}

func (m *mockApplyFuture) Error() error {
	return m.err
}

func (m *mockApplyFuture) Response() interface{} {
	return nil
}

func (m *mockApplyFuture) Index() uint64 {
	return 0
}

func TestStallMonitor_DetectsStall(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "fsm.db")

	fsm, err := New(Config{
		DBPath: dbPath,
		NodeID: "node-0",
	})
	if err != nil {
		t.Fatalf("failed to create FSM: %v", err)
	}
	defer fsm.Close()

	mockRaft := newMockRaftApplier(raft.Leader)

	builder := command.NewCommandBuilder()

	sealCmd := builder.BuildSegmentSealed(0, 1, 100,
		1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd})

	record, err := fsm.GetSegment(0)
	if err != nil {
		t.Fatalf("failed to get segment: %v", err)
	}

	record.SealedAt = uint64(time.Now().Add(-10 * time.Minute).UnixMicro())
	fsm.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)
		return b.Put(EncodeUint32(0), record.Encode())
	})

	config := StallMonitorConfig{
		StallTimeout:     1 * time.Minute,
		CheckInterval:    100 * time.Millisecond,
		MaxReassignments: 3,
		NodeSelector: func(excludeNode string) string {
			if excludeNode == "node-0" {
				return "node-1"
			}
			return "node-0"
		},
	}

	monitor := NewStallMonitor(fsm, mockRaft, config)
	monitor.ForceCheck(context.Background())

	commands := mockRaft.GetCommands()
	if len(commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(commands))
	}
}

func TestStallMonitor_SkipsNonLeader(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "fsm.db")

	fsm, err := New(Config{
		DBPath: dbPath,
		NodeID: "node-0",
	})
	if err != nil {
		t.Fatalf("failed to create FSM: %v", err)
	}
	defer fsm.Close()

	mockRaft := newMockRaftApplier(raft.Follower)

	builder := command.NewCommandBuilder()
	sealCmd := builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd})

	record, _ := fsm.GetSegment(0)
	record.SealedAt = uint64(time.Now().Add(-10 * time.Minute).UnixMicro())
	fsm.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)
		return b.Put(EncodeUint32(0), record.Encode())
	})

	config := StallMonitorConfig{
		StallTimeout:  1 * time.Minute,
		CheckInterval: 100 * time.Millisecond,
		NodeSelector: func(excludeNode string) string {
			return "node-1"
		},
	}

	monitor := NewStallMonitor(fsm, mockRaft, config)
	monitor.ForceCheck(context.Background())

	commands := mockRaft.GetCommands()
	if len(commands) != 0 {
		t.Errorf("expected 0 commands when not leader, got %d", len(commands))
	}
}

func TestStallMonitor_RespectsMaxReassignments(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "fsm.db")

	fsm, err := New(Config{
		DBPath: dbPath,
		NodeID: "node-0",
	})
	if err != nil {
		t.Fatalf("failed to create FSM: %v", err)
	}
	defer fsm.Close()

	mockRaft := newMockRaftApplier(raft.Leader)

	builder := command.NewCommandBuilder()
	sealCmd := builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd})
	record, _ := fsm.GetSegment(0)
	record.SealedAt = uint64(time.Now().Add(-10 * time.Minute).UnixMicro())
	record.AttemptCount = 2
	fsm.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)
		return b.Put(EncodeUint32(0), record.Encode())
	})

	config := StallMonitorConfig{
		StallTimeout:     1 * time.Minute,
		CheckInterval:    100 * time.Millisecond,
		MaxReassignments: 2,
		NodeSelector: func(excludeNode string) string {
			return "node-1"
		},
	}

	monitor := NewStallMonitor(fsm, mockRaft, config)
	monitor.ForceCheck(context.Background())

	commands := mockRaft.GetCommands()
	if len(commands) != 1 {
		t.Errorf("expected 1 SEGMENT_FAILED command when max reassignments reached, got %d", len(commands))
	}

	count := monitor.GetReassignmentCount(0)
	if count != 2 {
		t.Errorf("expected reassignment count=2 (unchanged), got %d", count)
	}
}

func TestStallMonitor_SkipsNotStalledSegments(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "fsm.db")

	fsm, err := New(Config{
		DBPath: dbPath,
		NodeID: "node-0",
	})
	if err != nil {
		t.Fatalf("failed to create FSM: %v", err)
	}
	defer fsm.Close()

	mockRaft := newMockRaftApplier(raft.Leader)

	builder := command.NewCommandBuilder()
	sealCmd := builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd})

	config := StallMonitorConfig{
		StallTimeout:  5 * time.Minute,
		CheckInterval: 100 * time.Millisecond,
		NodeSelector: func(excludeNode string) string {
			return "node-1"
		},
	}

	monitor := NewStallMonitor(fsm, mockRaft, config)
	monitor.ForceCheck(context.Background())

	commands := mockRaft.GetCommands()
	if len(commands) != 0 {
		t.Errorf("expected 0 commands for non-stalled segment, got %d", len(commands))
	}
}

func TestStallMonitor_SkipsUploadedSegments(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "fsm.db")

	fsm, err := New(Config{
		DBPath: dbPath,
		NodeID: "node-0",
	})
	if err != nil {
		t.Fatalf("failed to create FSM: %v", err)
	}
	defer fsm.Close()

	mockRaft := newMockRaftApplier(raft.Leader)

	builder := command.NewCommandBuilder()

	sealCmd0 := builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd0})
	uploadCmd := builder.BuildSegmentUploaded(0, "bucket", nil, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 102, Term: 1, Data: uploadCmd})

	sealCmd1 := builder.BuildSegmentSealed(1, 101, 200, 1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 103, Term: 1, Data: sealCmd1})
	record, _ := fsm.GetSegment(1)
	record.SealedAt = uint64(time.Now().Add(-10 * time.Minute).UnixMicro())
	fsm.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)
		return b.Put(EncodeUint32(1), record.Encode())
	})

	config := StallMonitorConfig{
		StallTimeout:  1 * time.Minute,
		CheckInterval: 100 * time.Millisecond,
		NodeSelector: func(excludeNode string) string {
			return "node-1"
		},
	}

	monitor := NewStallMonitor(fsm, mockRaft, config)
	monitor.ForceCheck(context.Background())
	commands := mockRaft.GetCommands()
	if len(commands) != 1 {
		t.Errorf("expected 1 command (only for sealed segment), got %d", len(commands))
	}
}

func TestStallMonitor_StartStop(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "fsm.db")

	fsm, err := New(Config{
		DBPath: dbPath,
		NodeID: "node-0",
	})
	if err != nil {
		t.Fatalf("failed to create FSM: %v", err)
	}
	defer fsm.Close()

	mockRaft := newMockRaftApplier(raft.Leader)

	config := StallMonitorConfig{
		StallTimeout:  5 * time.Minute,
		CheckInterval: 10 * time.Millisecond,
	}

	monitor := NewStallMonitor(fsm, mockRaft, config)
	monitor.Start()

	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		monitor.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("StallMonitor.Stop() timed out")
	}
}

func TestStallMonitor_UsesRaftConfigurationWhenNoNodeSelector(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "fsm.db")

	fsm, err := New(Config{
		DBPath: dbPath,
		NodeID: "node-0",
	})
	if err != nil {
		t.Fatalf("failed to create FSM: %v", err)
	}
	defer fsm.Close()

	mockRaft := newMockRaftApplier(raft.Leader)

	builder := command.NewCommandBuilder()
	sealCmd := builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd})

	record, _ := fsm.GetSegment(0)
	record.SealedAt = uint64(time.Now().Add(-10 * time.Minute).UnixMicro())
	fsm.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)
		return b.Put(EncodeUint32(0), record.Encode())
	})

	config := StallMonitorConfig{
		StallTimeout:  1 * time.Minute,
		CheckInterval: 100 * time.Millisecond,
	}

	monitor := NewStallMonitor(fsm, mockRaft, config)
	monitor.ForceCheck(context.Background())

	commands := mockRaft.GetCommands()
	if len(commands) != 1 {
		t.Errorf("expected 1 command when using Raft configuration, got %d", len(commands))
	}
}

func TestStallMonitor_NoAlternativeNodes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "fsm.db")

	fsm, err := New(Config{
		DBPath: dbPath,
		NodeID: "node-0",
	})
	if err != nil {
		t.Fatalf("failed to create FSM: %v", err)
	}
	defer fsm.Close()

	mockRaft := newMockRaftApplier(raft.Leader)
	mockRaft.SetServers([]raft.Server{
		{ID: "node-0", Suffrage: raft.Voter},
	})

	builder := command.NewCommandBuilder()
	sealCmd := builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd})

	record, _ := fsm.GetSegment(0)
	record.SealedAt = uint64(time.Now().Add(-10 * time.Minute).UnixMicro())
	fsm.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)
		return b.Put(EncodeUint32(0), record.Encode())
	})

	config := StallMonitorConfig{
		StallTimeout:  1 * time.Minute,
		CheckInterval: 100 * time.Millisecond,
	}

	monitor := NewStallMonitor(fsm, mockRaft, config)
	monitor.ForceCheck(context.Background())

	commands := mockRaft.GetCommands()
	if len(commands) != 0 {
		t.Errorf("expected 0 commands when no alternative nodes, got %d", len(commands))
	}
}
