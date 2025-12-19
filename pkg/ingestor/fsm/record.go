package fsm

import (
	"encoding/binary"
	"encoding/json"

	bolt "go.etcd.io/bbolt"
)

// Segment states
const (
	StateSealed   uint8 = 1
	StateUploaded uint8 = 2
	StateFailed   uint8 = 3
)

// SegmentRecord is the BoltDB value for a segment.
type SegmentRecord struct {
	// StateSealed, StateUploaded, or StateFailed
	State uint8 `json:"state"`
	// Raft ServerID (e.g., "node-1", "node-2")
	AssignedTo string `json:"assigned_to"`

	// First Raft log index in segment
	FirstIndex uint64 `json:"first_index"`
	// Last Raft log index in segment
	LastIndex uint64 `json:"last_index"`

	ByteSize   uint64 `json:"byte_size"`
	EntryCount uint64 `json:"entry_count"`

	// Timestamps (microseconds since epoch)
	SealedAt   uint64 `json:"sealed_at"`
	AssignedAt uint64 `json:"assigned_at,omitempty"`
	UploadedAt uint64 `json:"uploaded_at,omitempty"`
	SealedTerm uint64 `json:"sealed_term"`

	AttemptCount  uint32 `json:"attempt_count,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`

	// Object storage bucket URL
	BucketURL string `json:"bucket_url,omitempty"`
	// Number of parquet files written
	FileCount uint32 `json:"file_count,omitempty"`
	// Total bytes written to storage
	TotalBytes uint64 `json:"total_bytes,omitempty"`
	// Total rows written
	TotalRows uint64 `json:"total_rows,omitempty"`
}

// Encode serializes the SegmentRecord to JSON bytes.
func (r *SegmentRecord) Encode() []byte {
	data, err := json.Marshal(r)
	if err != nil {
		return nil
	}
	return data
}

func DecodeSegmentRecord(data []byte) *SegmentRecord {
	if len(data) == 0 {
		return nil
	}
	var r SegmentRecord
	if err := json.Unmarshal(data, &r); err != nil {
		return nil
	}
	return &r
}

// EncodeUint32 converts a uint32 to big-endian bytes for BoltDB keys.
func EncodeUint32(v uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	return buf
}

// DecodeUint32 converts big-endian bytes back to uint32.
func DecodeUint32(data []byte) uint32 {
	if len(data) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(data)
}

func EncodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

func DecodeUint64(data []byte) uint64 {
	if len(data) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

// CalculateSafeGCIndex finds the highest Raft log index that is safe to garbage collect.
//
// This is the last_index of the highest CONTIGUOUS UPLOADED segment.
// We can only GC logs up to this point because:
//   - SEALED segments need their log entries for processing
//   - Out-of-order uploads create holes that must be respected
//
// Example:
//
//	segments:
//	  seg0: UPLOADED, last_index=100  - contiguous
//	  seg1: UPLOADED, last_index=200  - contiguous
//	  seg2: SEALED,   last_index=300  - breaks chain
//	  seg3: UPLOADED, last_index=400  (doesn't matter - gap exists)
//
//	Returns: (200, term_of_seg1)
//	  - safe_to_gc_index = 200
//	  - Logs 1-200 can be compacted
//	  - Logs 201-400 must be preserved for seg2, seg3
func CalculateSafeGCIndex(tx *bolt.Tx) (index uint64, term uint64) {
	b := tx.Bucket(bucketSegments)
	if b == nil {
		return 0, 0
	}

	var safeIndex uint64 = 0
	var safeTerm uint64 = 0
	var expectedID uint32
	first := true

	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		segID := DecodeUint32(k)

		if first {
			expectedID = segID
			first = false
		}

		// should be contiguous from starting point
		if segID != expectedID {
			break
		}

		record := DecodeSegmentRecord(v)
		if record == nil {
			break
		}

		if record.State != StateUploaded {
			break
		}

		safeIndex = record.LastIndex
		safeTerm = record.SealedTerm
		expectedID++
	}

	return safeIndex, safeTerm
}

// CleanupContiguousUploaded removes UPLOADED segments that are now safe to forget.
func CleanupContiguousUploaded(tx *bolt.Tx, safeGCIndex uint64) error {
	b := tx.Bucket(bucketSegments)
	if b == nil {
		return nil
	}

	c := b.Cursor()
	for k, v := c.First(); k != nil; {
		record := DecodeSegmentRecord(v)
		if record == nil {
			break
		}

		// delete UPLOADED segments within safe range
		if record.State == StateUploaded && record.LastIndex <= safeGCIndex {
			if err := c.Delete(); err != nil {
				return err
			}
			k, v = c.First()
		} else {
			break
		}
	}

	return nil
}
