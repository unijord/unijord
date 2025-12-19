package fsm

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"

	raftfb "github.com/unijord/unijord/pkg/gen/go/fb/raft"
)

var (
	// we will store segments in this bucket.
	// key will be segment-id: 0,1,2,3
	// Btree has nice property of range scan
	// We will utilize this later for various task.
	bucketSegments = []byte("segments")
	// raft related meta db bucket
	bucketMeta = []byte("meta")
)

var (
	// we will override it to support the applied index and applied term
	// this will help in the segment deletion and log truncation.
	keyAppliedIndex = []byte("applied_index")
	keyAppliedTerm  = []byte("applied_term")
)

// SegmentCallback is called when a segment transitions to a new state.
// FSM Should call this so that notifier is aware and can start its processing.
type SegmentCallback func(segmentID uint32, state uint8, record *SegmentRecord)

// FSM implements the hashicorp/raft.FSM interface using BoltDB for Metadata state storage.
//
// The FSM tracks segment lifecycle:
//   - SEALED: Segment is closed, ready for processing
//   - UPLOADED: Parquet files uploaded to object storage
//
// IMP: safe_to_gc_index only advances for contiguous UPLOADED segments. This is to make sure
// when raft compact the logs, we never removes entries that is still needed.
type FSM struct {
	db           *bolt.DB
	dbPath       string
	safeGCHolder *SafeGCIndexHolder
	logger       *slog.Logger

	mu        sync.RWMutex
	callbacks []SegmentCallback

	// Node identity for processing assignment (Raft ServerID)
	nodeID string
}

// Config holds FSM configuration Options.
type Config struct {
	DBPath string
	// Raft ServerID (e.g., "node-1", "node-2")
	NodeID       string
	Logger       *slog.Logger
	SafeGCHolder *SafeGCIndexHolder
}

// New creates a new FSM backed by BoltDB for Metadata.
func New(cfg Config) (*FSM, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.SafeGCHolder == nil {
		cfg.SafeGCHolder = NewSafeGCIndexHolder()
	}

	db, err := bolt.Open(cfg.DBPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open boltdb: %w", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketSegments); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketMeta); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create buckets: %w", err)
	}

	fsm := &FSM{
		db:           db,
		dbPath:       cfg.DBPath,
		safeGCHolder: cfg.SafeGCHolder,
		logger:       cfg.Logger.With("component", "fsm"),
		nodeID:       cfg.NodeID,
	}

	// safe_to_gc_index from existing state
	if err := fsm.initializeSafeGCIndex(); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize safe_gc_index: %w", err)
	}

	return fsm, nil
}

func (f *FSM) initializeSafeGCIndex() error {
	return f.db.View(func(tx *bolt.Tx) error {
		index, term := CalculateSafeGCIndex(tx)
		if index > 0 {
			f.safeGCHolder.Set(index, term)
			f.logger.Info("initialized safe_to_gc_index",
				"index", index,
				"term", term)
		}
		return nil
	})
}

// RegisterCallback adds a callback for segment state changes.
func (f *FSM) RegisterCallback(cb SegmentCallback) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callbacks = append(f.callbacks, cb)
}

// notifyCallbacks invokes all registered callbacks.
func (f *FSM) notifyCallbacks(segmentID uint32, state uint8, record *SegmentRecord) {
	f.mu.RLock()
	callbacks := f.callbacks
	f.mu.RUnlock()

	for _, cb := range callbacks {
		cb(segmentID, state, record)
	}
}

// Apply implements raft.FSM.
func (f *FSM) Apply(log *raft.Log) interface{} {
	if len(log.Data) == 0 {
		return nil
	}

	cmd := raftfb.GetRootAsRaftCommand(log.Data, 0)

	switch cmd.Type() {
	case raftfb.CommandTypeSEGMENT_SEALED:
		return f.applySegmentSealed(cmd, log.Index, log.Term)

	case raftfb.CommandTypeSEGMENT_UPLOADED:
		return f.applySegmentUploaded(cmd)

	case raftfb.CommandTypeSEGMENT_REASSIGNED:
		return f.applySegmentReassigned(cmd)

	case raftfb.CommandTypeAPPEND:
		// NOOP we maintain this through the reader.
		return nil

	default:
		f.logger.Warn("unknown command type", "type", cmd.Type())
		return nil
	}
}

// applySegmentSealed handles SEGMENT_SEALED command.
func (f *FSM) applySegmentSealed(cmd *raftfb.RaftCommand, logIndex, logTerm uint64) interface{} {
	var table flatbuffers.Table
	if !cmd.Payload(&table) {
		f.logger.Error("failed to get payload from SEGMENT_SEALED")
		return fmt.Errorf("no payload in SEGMENT_SEALED")
	}

	var sealed raftfb.SegmentSealedCommand
	sealed.Init(table.Bytes, table.Pos)

	segmentID := sealed.SegmentId()

	record := &SegmentRecord{
		State:      StateSealed,
		AssignedTo: string(sealed.AssignedTo()),
		FirstIndex: sealed.FirstIndex(),
		LastIndex:  sealed.LastIndex(),
		ByteSize:   sealed.ByteSize(),
		EntryCount: sealed.EntryCount(),
		SealedAt:   sealed.SealedAt(),
		SealedTerm: logTerm,
	}

	err := f.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)

		// if already noop
		key := EncodeUint32(segmentID)
		if existing := b.Get(key); existing != nil {
			f.logger.Debug("segment already sealed, ignoring duplicate",
				"segment_id", segmentID)
			return nil
		}

		if err := b.Put(key, record.Encode()); err != nil {
			return err
		}

		meta := tx.Bucket(bucketMeta)
		if err := meta.Put(keyAppliedIndex, EncodeUint64(logIndex)); err != nil {
			return err
		}
		if err := meta.Put(keyAppliedTerm, EncodeUint64(logTerm)); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		f.logger.Error("failed to apply SEGMENT_SEALED",
			"segment_id", segmentID,
			"error", err)
		return err
	}

	f.logger.Info("segment sealed",
		"segment_id", segmentID,
		"assigned_to", record.AssignedTo,
		"first_index", record.FirstIndex,
		"last_index", record.LastIndex,
		"byte_size", record.ByteSize,
		"entry_count", record.EntryCount)

	f.notifyCallbacks(segmentID, StateSealed, record)

	return nil
}

// applySegmentUploaded handles SEGMENT_UPLOADED command.
func (f *FSM) applySegmentUploaded(cmd *raftfb.RaftCommand) interface{} {
	var table flatbuffers.Table
	if !cmd.Payload(&table) {
		f.logger.Error("failed to get payload from SEGMENT_UPLOADED")
		return fmt.Errorf("no payload in SEGMENT_UPLOADED")
	}

	var uploaded raftfb.SegmentUploadedCommand
	uploaded.Init(table.Bytes, table.Pos)

	segmentID := uploaded.SegmentId()

	var updatedRecord *SegmentRecord
	var newSafeIndex, newSafeTerm uint64

	err := f.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)
		key := EncodeUint32(segmentID)

		existing := b.Get(key)
		if existing == nil {
			// Segment doesn't exist - this shouldn't happen
			// TODO: Will it ever happen, with raft consensus ?
			// If ture How we handle it.
			f.logger.Warn("SEGMENT_UPLOADED for unknown segment",
				"segment_id", segmentID)
			return nil
		}

		record := DecodeSegmentRecord(existing)
		if record == nil {
			return fmt.Errorf("failed to decode segment record")
		}

		if record.State == StateUploaded {
			f.logger.Debug("segment already uploaded, ignoring duplicate",
				"segment_id", segmentID)
			return nil
		}

		// Change to UPLOADED state
		record.State = StateUploaded
		updatedRecord = record

		if err := b.Put(key, record.Encode()); err != nil {
			return err
		}
		newSafeIndex, newSafeTerm = CalculateSafeGCIndex(tx)

		return nil
	})

	if err != nil {
		f.logger.Error("failed to apply SEGMENT_UPLOADED",
			"segment_id", segmentID,
			"error", err)
		return err
	}

	if newSafeIndex > 0 {
		f.safeGCHolder.Set(newSafeIndex, newSafeTerm)
		f.logger.Debug("updated safe_to_gc_index",
			"index", newSafeIndex,
			"term", newSafeTerm)
	}

	f.logger.Info("segment uploaded",
		"segment_id", segmentID,
		"uploaded_by", string(uploaded.UploadedBy()),
		"total_rows", uploaded.TotalRows())

	if updatedRecord != nil {
		f.notifyCallbacks(segmentID, StateUploaded, updatedRecord)
	}

	return nil
}

// applySegmentReassigned handles SEGMENT_REASSIGNED command.
func (f *FSM) applySegmentReassigned(cmd *raftfb.RaftCommand) interface{} {
	var table flatbuffers.Table
	if !cmd.Payload(&table) {
		f.logger.Error("failed to get payload from SEGMENT_REASSIGNED")
		return fmt.Errorf("no payload in SEGMENT_REASSIGNED")
	}

	var reassigned raftfb.SegmentReassignedCommand
	reassigned.Init(table.Bytes, table.Pos)

	segmentID := reassigned.SegmentId()
	newNode := string(reassigned.NewNode())
	previousNode := string(reassigned.PreviousNode())
	reason := string(reassigned.Reason())

	var updatedRecord *SegmentRecord

	err := f.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)
		key := EncodeUint32(segmentID)
		existing := b.Get(key)
		if existing == nil {
			f.logger.Warn("SEGMENT_REASSIGNED for unknown segment",
				"segment_id", segmentID)
			return nil
		}

		record := DecodeSegmentRecord(existing)
		if record == nil {
			return fmt.Errorf("failed to decode segment record")
		}

		if record.State != StateSealed {
			f.logger.Debug("ignoring reassignment for non-SEALED segment",
				"segment_id", segmentID,
				"state", record.State)
			return nil
		}

		if record.AssignedTo == newNode {
			f.logger.Debug("segment already assigned to new node",
				"segment_id", segmentID,
				"new_node", newNode)
			return nil
		}

		record.AssignedTo = newNode
		updatedRecord = record

		return b.Put(key, record.Encode())
	})

	if err != nil {
		f.logger.Error("failed to apply SEGMENT_REASSIGNED",
			"segment_id", segmentID,
			"error", err)
		return err
	}

	if updatedRecord != nil {
		f.logger.Info("segment reassigned",
			"segment_id", segmentID,
			"previous_node", previousNode,
			"new_node", newNode,
			"reason", reason)

		f.notifyCallbacks(segmentID, StateSealed, updatedRecord)
	}

	return nil
}

// Snapshot implements raft.FSM.
// Returns a snapshot of the current FSM state for log compaction.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// Start a read transaction
	tx, err := f.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("begin snapshot tx: %w", err)
	}

	return &FSMSnapshot{
		tx:     tx,
		logger: f.logger,
	}, nil
}

// Restore implements raft.FSM.
// Restores the FSM state from a snapshot by replacing the BoltDB file.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if err := f.db.Close(); err != nil {
		return fmt.Errorf("close db: %w", err)
	}

	tmpPath := f.dbPath + ".tmp"
	out, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	if _, err := io.Copy(out, rc); err != nil {
		out.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write snapshot: %w", err)
	}

	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("sync snapshot: %w", err)
	}
	out.Close()

	if err := os.Rename(tmpPath, f.dbPath); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}

	db, err := bolt.Open(f.dbPath, 0600, nil)
	if err != nil {
		return fmt.Errorf("reopen db: %w", err)
	}
	f.db = db

	if err := f.initializeSafeGCIndex(); err != nil {
		return fmt.Errorf("reinitialize safe_gc_index: %w", err)
	}

	f.logger.Info("restored FSM from snapshot")

	return nil
}

// GetSegment retrieves a segment record by ID.
func (f *FSM) GetSegment(segmentID uint32) (*SegmentRecord, error) {
	var record *SegmentRecord

	err := f.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)
		data := b.Get(EncodeUint32(segmentID))
		if data == nil {
			return nil
		}
		record = DecodeSegmentRecord(data)
		return nil
	})

	return record, err
}

// GetSealedSegments returns all segments in SEALED state.
func (f *FSM) GetSealedSegments() ([]uint32, error) {
	var segments []uint32

	err := f.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			record := DecodeSegmentRecord(v)
			if record != nil && record.State == StateSealed {
				segments = append(segments, DecodeUint32(k))
			}
		}
		return nil
	})

	return segments, err
}

// GetSealedSegmentsForNode returns SEALED segments assigned to a specific node.
func (f *FSM) GetSealedSegmentsForNode(nodeID string) ([]uint32, error) {
	var segments []uint32

	err := f.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSegments)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			record := DecodeSegmentRecord(v)
			if record != nil && record.State == StateSealed && record.AssignedTo == nodeID {
				segments = append(segments, DecodeUint32(k))
			}
		}
		return nil
	})

	return segments, err
}

// GetSafeGCIndex returns the current safe-to-GC index and term.
func (f *FSM) GetSafeGCIndex() (index, term uint64) {
	return f.safeGCHolder.Get()
}

// SafeGCHolder returns the SafeGCIndexHolder for use with FSMSnapshotStore.
func (f *FSM) SafeGCHolder() *SafeGCIndexHolder {
	return f.safeGCHolder
}

// Close closes the FSM and its underlying BoltDB.
func (f *FSM) Close() error {
	return f.db.Close()
}

// FSMSnapshot implements raft.FSMSnapshot.
type FSMSnapshot struct {
	tx     *bolt.Tx
	logger *slog.Logger
}

// Persist writes the entire BoltDB database to the snapshot sink.
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer s.tx.Rollback()

	_, err := s.tx.WriteTo(sink)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("write snapshot: %w", err)
	}

	return sink.Close()
}

// Release releases the snapshot resources.
func (s *FSMSnapshot) Release() {
	s.tx.Rollback()
}

var _ raft.FSM = (*FSM)(nil)
