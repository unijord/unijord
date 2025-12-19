package fsm

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"

	"github.com/unijord/unijord/pkg/ingestor/command"
)

type emptyReader struct{}

func (emptyReader) Read(_ []byte) (int, error) { return 0, io.EOF }

type noopSnapshotSink struct{}

func (noopSnapshotSink) Write(p []byte) (int, error) { return len(p), nil }
func (noopSnapshotSink) Close() error                { return nil }
func (noopSnapshotSink) ID() string                  { return "noop" }
func (noopSnapshotSink) Cancel() error               { return nil }

type recordingSnapshotStore struct {
	lastCreateIndex uint64
	lastCreateTerm  uint64
	createCalls     int
}

func (s *recordingSnapshotStore) Create(
	_ raft.SnapshotVersion,
	index, term uint64,
	_ raft.Configuration,
	_ uint64,
	_ raft.Transport,
) (raft.SnapshotSink, error) {
	s.lastCreateIndex = index
	s.lastCreateTerm = term
	s.createCalls++
	return noopSnapshotSink{}, nil
}

func (s *recordingSnapshotStore) List() ([]*raft.SnapshotMeta, error) { return nil, nil }

func (s *recordingSnapshotStore) Open(_ string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	return nil, io.NopCloser(emptyReader{}), nil
}

func TestFSMSnapshotStore_Create_OverridesIndexTerm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		safeIndex    uint64
		safeTerm     uint64
		appliedIndex uint64
		appliedTerm  uint64
		wantIndex    uint64
		wantTerm     uint64
	}{
		{
			name:         "overrides_with_safe_gc_when_valid",
			safeIndex:    200,
			safeTerm:     5,
			appliedIndex: 400,
			appliedTerm:  7,
			wantIndex:    200,
			wantTerm:     5,
		},
		{
			name:         "uses_applied_when_safe_gc_unset",
			safeIndex:    0,
			safeTerm:     0,
			appliedIndex: 400,
			appliedTerm:  7,
			wantIndex:    400,
			wantTerm:     7,
		},
		{
			name:         "uses_applied_when_safe_gc_ahead_of_applied",
			safeIndex:    300,
			safeTerm:     5,
			appliedIndex: 200,
			appliedTerm:  7,
			wantIndex:    200,
			wantTerm:     7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			holder := NewSafeGCIndexHolder()
			if tt.safeIndex > 0 {
				holder.Set(tt.safeIndex, tt.safeTerm)
			}

			inner := &recordingSnapshotStore{}
			store := NewFSMSnapshotStore(inner, holder)

			_, err := store.Create(
				raft.SnapshotVersionMax,
				tt.appliedIndex,
				tt.appliedTerm,
				raft.Configuration{},
				0,
				nil,
			)
			if err != nil {
				t.Fatalf("Create() error: %v", err)
			}
			if inner.createCalls != 1 {
				t.Fatalf("expected 1 Create call, got %d", inner.createCalls)
			}
			if inner.lastCreateIndex != tt.wantIndex || inner.lastCreateTerm != tt.wantTerm {
				t.Fatalf("Create() called with index/term=%d/%d, want %d/%d",
					inner.lastCreateIndex, inner.lastCreateTerm, tt.wantIndex, tt.wantTerm)
			}
		})
	}
}

func TestFSM_SegmentSealed(t *testing.T) {
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

	builder := command.NewCommandBuilder()
	cmdData := builder.BuildSegmentSealed(
		0,
		1,
		100,
		1024,
		50,
		"node-0",
	)

	log := &raft.Log{
		Index: 101,
		Term:  1,
		Data:  cmdData,
	}
	result := fsm.Apply(log)
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}

	record, err := fsm.GetSegment(0)
	if err != nil {
		t.Fatalf("failed to get segment: %v", err)
	}
	if record == nil {
		t.Fatal("expected segment record, got nil")
	}
	if record.State != StateSealed {
		t.Errorf("expected StateSealed, got %d", record.State)
	}
	if record.FirstIndex != 1 {
		t.Errorf("expected FirstIndex=1, got %d", record.FirstIndex)
	}
	if record.LastIndex != 100 {
		t.Errorf("expected LastIndex=100, got %d", record.LastIndex)
	}
	if record.SealedTerm != 1 {
		t.Errorf("expected SealedTerm=1, got %d", record.SealedTerm)
	}
}

func TestFSM_SegmentUploaded(t *testing.T) {
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

	builder := command.NewCommandBuilder()

	sealCmd := builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd})

	uploadCmd := builder.BuildSegmentUploaded(
		0,
		"test-bucket",
		[]command.ParquetFile{
			{SchemaID: 1, ObjectKey: "path/to/file.parquet", SizeBytes: 1000, RowCount: 50},
		},
		50,
		"node-0",
	)

	result := fsm.Apply(&raft.Log{Index: 102, Term: 1, Data: uploadCmd})
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}

	record, err := fsm.GetSegment(0)
	if err != nil {
		t.Fatalf("failed to get segment: %v", err)
	}
	if record == nil {
		t.Fatal("expected segment record, got nil")
	}
	if record.State != StateUploaded {
		t.Errorf("expected StateUploaded, got %d", record.State)
	}

	safeIndex, safeTerm := fsm.GetSafeGCIndex()
	if safeIndex != 100 {
		t.Errorf("expected safe_to_gc_index=100, got %d", safeIndex)
	}
	if safeTerm != 1 {
		t.Errorf("expected safe_to_gc_term=1, got %d", safeTerm)
	}
}

func TestFSM_ContiguousUploadTracking(t *testing.T) {
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

	builder := command.NewCommandBuilder()

	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")})
	fsm.Apply(&raft.Log{Index: 201, Term: 1, Data: builder.BuildSegmentSealed(1, 101, 200, 1024, 50, "node-1")})
	fsm.Apply(&raft.Log{Index: 301, Term: 1, Data: builder.BuildSegmentSealed(2, 201, 300, 1024, 50, "node-2")})

	fsm.Apply(&raft.Log{Index: 302, Term: 1, Data: builder.BuildSegmentUploaded(0, "bucket", nil, 50, "node-0")})

	safeIndex, _ := fsm.GetSafeGCIndex()
	if safeIndex != 100 {
		t.Errorf("expected safe_to_gc_index=100 after seg0 upload, got %d", safeIndex)
	}

	fsm.Apply(&raft.Log{Index: 303, Term: 1, Data: builder.BuildSegmentUploaded(2, "bucket", nil, 50, "node-2")})

	safeIndex, _ = fsm.GetSafeGCIndex()
	if safeIndex != 100 {
		t.Errorf("expected safe_to_gc_index=100 with hole at seg1, got %d", safeIndex)
	}

	fsm.Apply(&raft.Log{Index: 304, Term: 1, Data: builder.BuildSegmentUploaded(1, "bucket", nil, 50, "node-1")})

	safeIndex, _ = fsm.GetSafeGCIndex()
	if safeIndex != 300 {
		t.Errorf("expected safe_to_gc_index=300 after filling hole, got %d", safeIndex)
	}
}

func TestFSM_GetSealedSegments(t *testing.T) {
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

	builder := command.NewCommandBuilder()

	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")})
	fsm.Apply(&raft.Log{Index: 201, Term: 1, Data: builder.BuildSegmentSealed(1, 101, 200, 1024, 50, "node-1")})
	fsm.Apply(&raft.Log{Index: 301, Term: 1, Data: builder.BuildSegmentSealed(2, 201, 300, 1024, 50, "node-2")})

	fsm.Apply(&raft.Log{Index: 302, Term: 1, Data: builder.BuildSegmentUploaded(0, "bucket", nil, 50, "node-0")})

	sealed, err := fsm.GetSealedSegments()
	if err != nil {
		t.Fatalf("failed to get sealed segments: %v", err)
	}
	if len(sealed) != 2 {
		t.Errorf("expected 2 sealed segments, got %d", len(sealed))
	}

	sealedForNode1, err := fsm.GetSealedSegmentsForNode("node-1")
	if err != nil {
		t.Fatalf("failed to get sealed segments for node 1: %v", err)
	}
	if len(sealedForNode1) != 1 {
		t.Errorf("expected 1 sealed segment for node 1, got %d", len(sealedForNode1))
	}
	if len(sealedForNode1) > 0 && sealedForNode1[0] != 1 {
		t.Errorf("expected segment ID 1 for node 1, got %d", sealedForNode1[0])
	}
}

func TestFSM_Idempotency(t *testing.T) {
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

	builder := command.NewCommandBuilder()

	sealCmd := builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd})

	result := fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd})
	if result != nil {
		t.Errorf("expected nil result for duplicate seal, got %v", result)
	}

	uploadCmd := builder.BuildSegmentUploaded(0, "bucket", nil, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 102, Term: 1, Data: uploadCmd})

	result = fsm.Apply(&raft.Log{Index: 102, Term: 1, Data: uploadCmd})
	if result != nil {
		t.Errorf("expected nil result for duplicate upload, got %v", result)
	}

	record, err := fsm.GetSegment(0)
	if err != nil {
		t.Fatalf("failed to get segment: %v", err)
	}
	if record.State != StateUploaded {
		t.Errorf("expected StateUploaded, got %d", record.State)
	}
}

func TestFSM_Callback(t *testing.T) {
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

	var callbackCalled bool
	var callbackSegmentID uint32
	var callbackState uint8

	fsm.RegisterCallback(func(segmentID uint32, state uint8, record *SegmentRecord) {
		callbackCalled = true
		callbackSegmentID = segmentID
		callbackState = state
	})

	builder := command.NewCommandBuilder()
	sealCmd := builder.BuildSegmentSealed(5, 1, 100, 1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd})

	if !callbackCalled {
		t.Error("callback was not called")
	}
	if callbackSegmentID != 5 {
		t.Errorf("expected segmentID=5, got %d", callbackSegmentID)
	}
	if callbackState != StateSealed {
		t.Errorf("expected StateSealed, got %d", callbackState)
	}
}

func TestFSM_SnapshotRestore(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath1 := filepath.Join(tmpDir, "fsm1.db")
	dbPath2 := filepath.Join(tmpDir, "fsm2.db")

	fsm1, err := New(Config{
		DBPath: dbPath1,
		NodeID: "node-0",
	})
	if err != nil {
		t.Fatalf("failed to create FSM1: %v", err)
	}
	defer fsm1.Close()

	builder := command.NewCommandBuilder()

	fsm1.Apply(&raft.Log{Index: 101, Term: 1, Data: builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")})
	fsm1.Apply(&raft.Log{Index: 201, Term: 1, Data: builder.BuildSegmentSealed(1, 101, 200, 1024, 50, "node-1")})
	fsm1.Apply(&raft.Log{Index: 202, Term: 1, Data: builder.BuildSegmentUploaded(0, "bucket", nil, 50, "node-0")})

	snapshot, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	snapshotPath := filepath.Join(tmpDir, "snapshot")
	snapshotFile, err := os.Create(snapshotPath)
	if err != nil {
		t.Fatalf("failed to create snapshot file: %v", err)
	}

	sink := &mockSnapshotSink{file: snapshotFile}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("failed to persist snapshot: %v", err)
	}
	snapshotFile.Close()

	fsm2, err := New(Config{
		DBPath: dbPath2,
		NodeID: "node-0",
	})
	if err != nil {
		t.Fatalf("failed to create FSM2: %v", err)
	}
	defer fsm2.Close()

	var callbackCount int
	fsm2.RegisterCallback(func(_ uint32, _ uint8, _ *SegmentRecord) {
		callbackCount++
	})

	snapshotData, err := os.Open(snapshotPath)
	if err != nil {
		t.Fatalf("failed to open snapshot: %v", err)
	}
	if err := fsm2.Restore(snapshotData); err != nil {
		t.Fatalf("failed to restore snapshot: %v", err)
	}

	if callbackCount != 0 {
		t.Fatalf("expected no callbacks during Restore, got %d", callbackCount)
	}

	record, err := fsm2.GetSegment(0)
	if err != nil {
		t.Fatalf("failed to get segment: %v", err)
	}
	if record == nil {
		t.Fatal("expected segment 0, got nil")
	}
	if record.State != StateUploaded {
		t.Errorf("expected StateUploaded, got %d", record.State)
	}

	record, err = fsm2.GetSegment(1)
	if err != nil {
		t.Fatalf("failed to get segment: %v", err)
	}
	if record == nil {
		t.Fatal("expected segment 1, got nil")
	}
	if record.State != StateSealed {
		t.Errorf("expected StateSealed, got %d", record.State)
	}

	safeIndex, safeTerm := fsm2.GetSafeGCIndex()
	if safeIndex != 100 {
		t.Errorf("expected safe_to_gc_index=100, got %d", safeIndex)
	}
	if safeTerm != 1 {
		t.Errorf("expected safe_to_gc_term=1, got %d", safeTerm)
	}

	var appliedIndex, appliedTerm uint64
	if err := fsm2.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		if meta == nil {
			return errors.New("missing meta bucket")
		}
		appliedIndexBytes := meta.Get(keyAppliedIndex)
		appliedTermBytes := meta.Get(keyAppliedTerm)
		if appliedIndexBytes == nil || appliedTermBytes == nil {
			return errors.New("missing applied index/term")
		}
		appliedIndex = DecodeUint64(appliedIndexBytes)
		appliedTerm = DecodeUint64(appliedTermBytes)
		return nil
	}); err != nil {
		t.Fatalf("failed reading applied index/term: %v", err)
	}
	if appliedIndex != 201 {
		t.Errorf("expected applied_index=201, got %d", appliedIndex)
	}
	if appliedTerm != 1 {
		t.Errorf("expected applied_term=1, got %d", appliedTerm)
	}
}

type mockSnapshotSink struct {
	file *os.File
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	return m.file.Write(p)
}

func (m *mockSnapshotSink) Close() error {
	return m.file.Close()
}

func (m *mockSnapshotSink) ID() string {
	return "test-snapshot"
}

func (m *mockSnapshotSink) Cancel() error {
	return nil
}

func TestFSMSnapshotStore_RaftCompactionRespectsSafeGC(t *testing.T) {
	tmpDir := t.TempDir()

	holder := NewSafeGCIndexHolder()
	fsm, err := New(Config{
		DBPath:       filepath.Join(tmpDir, "fsm.db"),
		NodeID:       "node-0",
		SafeGCHolder: holder,
	})
	if err != nil {
		t.Fatalf("failed to create FSM: %v", err)
	}
	defer fsm.Close()

	innerLogStore := raft.NewInmemStore()
	logStore := NewSafeGCLogStore(innerLogStore, holder)
	stableStore := raft.NewInmemStore()
	baseSnapshots := raft.NewInmemSnapshotStore()
	snapshots := NewFSMSnapshotStore(baseSnapshots, holder)

	addr, transport := raft.NewInmemTransport("")
	defer transport.Close()

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(addr)
	cfg.Logger = hclog.NewNullLogger()
	cfg.SnapshotInterval = 24 * time.Hour
	cfg.SnapshotThreshold = 1_000_000
	cfg.TrailingLogs = 0

	clusterConfig := raft.Configuration{
		Servers: []raft.Server{
			{Suffrage: raft.Voter, ID: cfg.LocalID, Address: addr},
		},
	}
	if err := raft.BootstrapCluster(cfg, logStore, stableStore, snapshots, transport, clusterConfig); err != nil {
		t.Fatalf("BootstrapCluster failed: %v", err)
	}

	r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		t.Fatalf("NewRaft failed: %v", err)
	}
	defer func() { _ = r.Shutdown().Error() }()

	deadline := time.Now().Add(5 * time.Second)
	for r.State() != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for leader, state=%v", r.State())
		}
		time.Sleep(10 * time.Millisecond)
	}

	builder := command.NewCommandBuilder()
	appendCmd := builder.BuildAppend([][]byte{{0x1}})

	var appliedIndex uint64
	for appliedIndex < 400 {
		f := r.Apply(appendCmd, 5*time.Second)
		if err := f.Error(); err != nil {
			t.Fatalf("Apply error at index %d: %v", appliedIndex, err)
		}
		appliedIndex = f.Index()
	}

	holder.Set(200, 5)

	sf := r.Snapshot()
	if err := sf.Error(); err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	meta, rc, err := sf.Open()
	if err != nil {
		t.Fatalf("Snapshot open error: %v", err)
	}
	rc.Close()
	if meta.Index != 200 {
		t.Fatalf("expected snapshot meta index=200, got %d", meta.Index)
	}

	compactionDeadline := time.Now().Add(5 * time.Second)
	for {
		first, err := logStore.FirstIndex()
		if err != nil {
			t.Fatalf("FirstIndex error: %v", err)
		}
		if first == 201 {
			break
		}
		if time.Now().After(compactionDeadline) {
			last, _ := logStore.LastIndex()
			t.Fatalf("timed out waiting for compaction; FirstIndex=%d, LastIndex=%d", first, last)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := logStore.GetLog(200, &raft.Log{}); err == nil {
		t.Fatalf("expected log 200 to be compacted, but it still exists")
	}
	if err := logStore.GetLog(201, &raft.Log{}); err != nil {
		t.Fatalf("expected log 201 to exist after snapshot: %v", err)
	}
	if err := logStore.GetLog(400, &raft.Log{}); err != nil {
		t.Fatalf("expected log 400 to exist after snapshot: %v", err)
	}
}

func TestFSM_SegmentReassigned(t *testing.T) {
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

	builder := command.NewCommandBuilder()

	sealCmd := builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd})

	record, err := fsm.GetSegment(0)
	if err != nil {
		t.Fatalf("failed to get segment: %v", err)
	}
	if record.AssignedTo != "node-0" {
		t.Errorf("expected AssignedTo=node-0, got %s", record.AssignedTo)
	}

	reassignCmd := builder.BuildSegmentReassigned(0, "node-0", "node-1", "timeout")
	result := fsm.Apply(&raft.Log{Index: 102, Term: 1, Data: reassignCmd})
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}

	record, err = fsm.GetSegment(0)
	if err != nil {
		t.Fatalf("failed to get segment: %v", err)
	}
	if record.AssignedTo != "node-1" {
		t.Errorf("expected AssignedTo=node-1 after reassignment, got %s", record.AssignedTo)
	}

	if record.State != StateSealed {
		t.Errorf("expected StateSealed, got %d", record.State)
	}

	result = fsm.Apply(&raft.Log{Index: 103, Term: 1, Data: reassignCmd})
	if result != nil {
		t.Errorf("expected nil result for duplicate reassign, got %v", result)
	}

	segmentsForNode0, err := fsm.GetSealedSegmentsForNode("node-0")
	if err != nil {
		t.Fatalf("failed to get sealed segments for node 0: %v", err)
	}
	if len(segmentsForNode0) != 0 {
		t.Errorf("expected 0 segments for node 0, got %d", len(segmentsForNode0))
	}

	segmentsForNode1, err := fsm.GetSealedSegmentsForNode("node-1")
	if err != nil {
		t.Fatalf("failed to get sealed segments for node 1: %v", err)
	}
	if len(segmentsForNode1) != 1 {
		t.Errorf("expected 1 segment for node 1, got %d", len(segmentsForNode1))
	}
}

func TestFSM_SegmentReassignedCallback(t *testing.T) {
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

	var callbackCount int
	var lastSegmentID uint32
	var lastState uint8
	var lastAssignedTo string

	fsm.RegisterCallback(func(segmentID uint32, state uint8, record *SegmentRecord) {
		callbackCount++
		lastSegmentID = segmentID
		lastState = state
		lastAssignedTo = record.AssignedTo
	})

	builder := command.NewCommandBuilder()

	sealCmd := builder.BuildSegmentSealed(0, 1, 100, 1024, 50, "node-0")
	fsm.Apply(&raft.Log{Index: 101, Term: 1, Data: sealCmd})

	if callbackCount != 1 {
		t.Errorf("expected 1 callback after seal, got %d", callbackCount)
	}
	if lastAssignedTo != "node-0" {
		t.Errorf("expected lastAssignedTo=node-0 after seal, got %s", lastAssignedTo)
	}

	reassignCmd := builder.BuildSegmentReassigned(0, "node-0", "node-2", "node_down")
	fsm.Apply(&raft.Log{Index: 102, Term: 1, Data: reassignCmd})

	if callbackCount != 2 {
		t.Errorf("expected 2 callbacks after reassign, got %d", callbackCount)
	}
	if lastSegmentID != 0 {
		t.Errorf("expected lastSegmentID=0, got %d", lastSegmentID)
	}
	if lastState != StateSealed {
		t.Errorf("expected lastState=StateSealed, got %d", lastState)
	}
	if lastAssignedTo != "node-2" {
		t.Errorf("expected lastAssignedTo=node-2 after reassign, got %s", lastAssignedTo)
	}
}
