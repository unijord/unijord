package fsm

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestCalculateSafeGCIndex(t *testing.T) {
	tests := []struct {
		name          string
		setupSegments func(tx *bolt.Tx) error
		wantIndex     uint64
		wantTerm      uint64
	}{
		{
			name:          "no segments bucket",
			setupSegments: nil,
			wantIndex:     0,
			wantTerm:      0,
		},
		{
			name: "empty segments bucket",
			setupSegments: func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket([]byte("segments"))
				return err
			},
			wantIndex: 0,
			wantTerm:  0,
		},
		{
			name: "single UPLOADED segment",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket([]byte("segments"))
				if err != nil {
					return err
				}
				rec := &SegmentRecord{
					State:      StateUploaded,
					FirstIndex: 1,
					LastIndex:  100,
					SealedTerm: 1,
				}
				return b.Put(EncodeUint32(0), rec.Encode())
			},
			wantIndex: 100,
			wantTerm:  1,
		},
		{
			name: "multiple contiguous UPLOADED segments",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket([]byte("segments"))
				if err != nil {
					return err
				}
				segments := []struct {
					id         uint32
					firstIndex uint64
					lastIndex  uint64
					term       uint64
				}{
					{0, 1, 100, 1},
					{1, 101, 200, 1},
					{2, 201, 300, 2},
				}
				for _, seg := range segments {
					rec := &SegmentRecord{
						State:      StateUploaded,
						FirstIndex: seg.firstIndex,
						LastIndex:  seg.lastIndex,
						SealedTerm: seg.term,
					}
					if err := b.Put(EncodeUint32(seg.id), rec.Encode()); err != nil {
						return err
					}
				}
				return nil
			},
			wantIndex: 300,
			wantTerm:  2,
		},
		{
			name: "first segment is SEALED (not uploaded)",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket([]byte("segments"))
				if err != nil {
					return err
				}
				rec := &SegmentRecord{
					State:      StateSealed,
					FirstIndex: 1,
					LastIndex:  100,
					SealedTerm: 1,
				}
				return b.Put(EncodeUint32(0), rec.Encode())
			},
			wantIndex: 0,
			wantTerm:  0,
		},
		{
			name: "UPLOADED then SEALED - stops at SEALED",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket([]byte("segments"))
				if err != nil {
					return err
				}

				rec0 := &SegmentRecord{
					State:      StateUploaded,
					FirstIndex: 1,
					LastIndex:  100,
					SealedTerm: 1,
				}
				if err := b.Put(EncodeUint32(0), rec0.Encode()); err != nil {
					return err
				}

				rec1 := &SegmentRecord{
					State:      StateSealed,
					FirstIndex: 101,
					LastIndex:  200,
					SealedTerm: 1,
				}
				return b.Put(EncodeUint32(1), rec1.Encode())
			},
			wantIndex: 100,
			wantTerm:  1,
		},
		{
			name: "hole in segment IDs - gap stops GC",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket([]byte("segments"))
				if err != nil {
					return err
				}

				rec0 := &SegmentRecord{
					State:      StateUploaded,
					FirstIndex: 1,
					LastIndex:  100,
					SealedTerm: 1,
				}
				if err := b.Put(EncodeUint32(0), rec0.Encode()); err != nil {
					return err
				}

				rec2 := &SegmentRecord{
					State:      StateUploaded,
					FirstIndex: 201,
					LastIndex:  300,
					SealedTerm: 2,
				}
				return b.Put(EncodeUint32(2), rec2.Encode())
			},
			wantIndex: 100,
			wantTerm:  1,
		},
		{
			name: "UPLOADED SEALED UPLOADED - hole blocks later uploads",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket([]byte("segments"))
				if err != nil {
					return err
				}

				rec0 := &SegmentRecord{
					State:      StateUploaded,
					FirstIndex: 1,
					LastIndex:  100,
					SealedTerm: 1,
				}
				if err := b.Put(EncodeUint32(0), rec0.Encode()); err != nil {
					return err
				}

				rec1 := &SegmentRecord{
					State:      StateSealed,
					FirstIndex: 101,
					LastIndex:  200,
					SealedTerm: 1,
				}
				if err := b.Put(EncodeUint32(1), rec1.Encode()); err != nil {
					return err
				}

				rec2 := &SegmentRecord{
					State:      StateUploaded,
					FirstIndex: 201,
					LastIndex:  300,
					SealedTerm: 2,
				}
				return b.Put(EncodeUint32(2), rec2.Encode())
			},
			wantIndex: 100,
			wantTerm:  1,
		},
		{
			name: "segments start from ID 1 after cleanup",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket([]byte("segments"))
				if err != nil {
					return err
				}

				rec1 := &SegmentRecord{
					State:      StateUploaded,
					FirstIndex: 101,
					LastIndex:  200,
					SealedTerm: 1,
				}
				if err := b.Put(EncodeUint32(1), rec1.Encode()); err != nil {
					return err
				}
				rec2 := &SegmentRecord{
					State:      StateUploaded,
					FirstIndex: 201,
					LastIndex:  300,
					SealedTerm: 2,
				}
				return b.Put(EncodeUint32(2), rec2.Encode())
			},
			wantIndex: 300,
			wantTerm:  2,
		},
		{
			name: "segments start from ID 5 with gap",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket([]byte("segments"))
				if err != nil {
					return err
				}

				for _, seg := range []struct {
					id    uint32
					last  uint64
					term  uint64
					state uint8
				}{
					{5, 500, 1, StateUploaded},
					{6, 600, 1, StateUploaded},

					{8, 800, 2, StateUploaded},
				} {
					rec := &SegmentRecord{
						State:      seg.state,
						FirstIndex: seg.last - 99,
						LastIndex:  seg.last,
						SealedTerm: seg.term,
					}
					if err := b.Put(EncodeUint32(seg.id), rec.Encode()); err != nil {
						return err
					}
				}
				return nil
			},
			wantIndex: 600,
			wantTerm:  1,
		},
		{
			name: "FAILED segment blocks GC",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket([]byte("segments"))
				if err != nil {
					return err
				}

				rec0 := &SegmentRecord{
					State:      StateUploaded,
					FirstIndex: 1,
					LastIndex:  100,
					SealedTerm: 1,
				}
				if err := b.Put(EncodeUint32(0), rec0.Encode()); err != nil {
					return err
				}

				rec1 := &SegmentRecord{
					State:         StateFailed,
					FirstIndex:    101,
					LastIndex:     200,
					SealedTerm:    1,
					FailureReason: "upload timeout",
				}
				return b.Put(EncodeUint32(1), rec1.Encode())
			},
			wantIndex: 100,
			wantTerm:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			dbPath := filepath.Join(t.TempDir(), "test.db")
			db, err := bolt.Open(dbPath, 0600, nil)
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer db.Close()

			if tt.setupSegments != nil {
				err = db.Update(tt.setupSegments)
				if err != nil {
					t.Fatalf("setup segments: %v", err)
				}
			}

			var gotIndex, gotTerm uint64
			err = db.View(func(tx *bolt.Tx) error {
				gotIndex, gotTerm = CalculateSafeGCIndex(tx)
				return nil
			})
			if err != nil {
				t.Fatalf("view: %v", err)
			}

			if gotIndex != tt.wantIndex {
				t.Errorf("index = %d, want %d", gotIndex, tt.wantIndex)
			}
			if gotTerm != tt.wantTerm {
				t.Errorf("term = %d, want %d", gotTerm, tt.wantTerm)
			}
		})
	}
}

func TestCleanupContiguousUploaded(t *testing.T) {
	tests := []struct {
		name          string
		setupSegments func(tx *bolt.Tx) error
		safeGCIndex   uint64
		wantRemaining []uint32
	}{
		{
			name:          "no segments bucket",
			setupSegments: nil,
			safeGCIndex:   100,
			wantRemaining: nil,
		},
		{
			name: "empty segments bucket",
			setupSegments: func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket(bucketSegments)
				return err
			},
			safeGCIndex:   100,
			wantRemaining: []uint32{},
		},
		{
			name: "single UPLOADED within range - deleted",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket(bucketSegments)
				if err != nil {
					return err
				}
				rec := &SegmentRecord{
					State:     StateUploaded,
					LastIndex: 100,
				}
				return b.Put(EncodeUint32(0), rec.Encode())
			},
			safeGCIndex:   100,
			wantRemaining: []uint32{},
		},
		{
			name: "single UPLOADED beyond range - kept",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket(bucketSegments)
				if err != nil {
					return err
				}
				rec := &SegmentRecord{
					State:     StateUploaded,
					LastIndex: 200,
				}
				return b.Put(EncodeUint32(0), rec.Encode())
			},
			safeGCIndex:   100,
			wantRemaining: []uint32{0},
		},
		{
			name: "single SEALED within range - kept (not UPLOADED)",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket(bucketSegments)
				if err != nil {
					return err
				}
				rec := &SegmentRecord{
					State:     StateSealed,
					LastIndex: 50,
				}
				return b.Put(EncodeUint32(0), rec.Encode())
			},
			safeGCIndex:   100,
			wantRemaining: []uint32{0},
		},
		{
			name: "multiple contiguous UPLOADED within range - all deleted",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket(bucketSegments)
				if err != nil {
					return err
				}
				for i, lastIdx := range []uint64{100, 200, 300} {
					rec := &SegmentRecord{
						State:     StateUploaded,
						LastIndex: lastIdx,
					}
					if err := b.Put(EncodeUint32(uint32(i)), rec.Encode()); err != nil {
						return err
					}
				}
				return nil
			},
			safeGCIndex:   300,
			wantRemaining: []uint32{},
		},
		{
			name: "UPLOADED UPLOADED SEALED - deletes first two, keeps SEALED",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket(bucketSegments)
				if err != nil {
					return err
				}
				segments := []struct {
					id    uint32
					state uint8
					last  uint64
				}{
					{0, StateUploaded, 100},
					{1, StateUploaded, 200},
					{2, StateSealed, 300},
				}
				for _, seg := range segments {
					rec := &SegmentRecord{
						State:     seg.state,
						LastIndex: seg.last,
					}
					if err := b.Put(EncodeUint32(seg.id), rec.Encode()); err != nil {
						return err
					}
				}
				return nil
			},
			safeGCIndex:   300,
			wantRemaining: []uint32{2},
		},
		{
			name: "first UPLOADED beyond range - nothing deleted",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket(bucketSegments)
				if err != nil {
					return err
				}
				segments := []struct {
					id   uint32
					last uint64
				}{
					{0, 150},
					{1, 200},
					{2, 300},
				}
				for _, seg := range segments {
					rec := &SegmentRecord{
						State:     StateUploaded,
						LastIndex: seg.last,
					}
					if err := b.Put(EncodeUint32(seg.id), rec.Encode()); err != nil {
						return err
					}
				}
				return nil
			},
			safeGCIndex:   100,
			wantRemaining: []uint32{0, 1, 2},
		},
		{
			name: "partial cleanup - some within range, some beyond",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket(bucketSegments)
				if err != nil {
					return err
				}
				segments := []struct {
					id   uint32
					last uint64
				}{
					{0, 100},
					{1, 200},
					{2, 300},
					{3, 400},
				}
				for _, seg := range segments {
					rec := &SegmentRecord{
						State:     StateUploaded,
						LastIndex: seg.last,
					}
					if err := b.Put(EncodeUint32(seg.id), rec.Encode()); err != nil {
						return err
					}
				}
				return nil
			},
			safeGCIndex:   250,
			wantRemaining: []uint32{2, 3},
		},
		{
			name: "FAILED segment stops cleanup",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket(bucketSegments)
				if err != nil {
					return err
				}
				segments := []struct {
					id    uint32
					state uint8
					last  uint64
				}{
					{0, StateUploaded, 100},
					{1, StateFailed, 200},
					{2, StateUploaded, 300},
				}
				for _, seg := range segments {
					rec := &SegmentRecord{
						State:     seg.state,
						LastIndex: seg.last,
					}
					if err := b.Put(EncodeUint32(seg.id), rec.Encode()); err != nil {
						return err
					}
				}
				return nil
			},
			safeGCIndex:   300,
			wantRemaining: []uint32{1, 2},
		},
		{
			name: "exact boundary - LastIndex equals safeGCIndex",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket(bucketSegments)
				if err != nil {
					return err
				}
				rec := &SegmentRecord{
					State:     StateUploaded,
					LastIndex: 100,
				}
				return b.Put(EncodeUint32(0), rec.Encode())
			},
			safeGCIndex:   100,
			wantRemaining: []uint32{},
		},
		{
			name: "one below boundary - LastIndex is safeGCIndex+1",
			setupSegments: func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket(bucketSegments)
				if err != nil {
					return err
				}
				rec := &SegmentRecord{
					State:     StateUploaded,
					LastIndex: 101,
				}
				return b.Put(EncodeUint32(0), rec.Encode())
			},
			safeGCIndex:   100,
			wantRemaining: []uint32{0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "test.db")
			db, err := bolt.Open(dbPath, 0600, nil)
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer db.Close()

			if tt.setupSegments != nil {
				if err := db.Update(tt.setupSegments); err != nil {
					t.Fatalf("setup: %v", err)
				}
			}

			err = db.Update(func(tx *bolt.Tx) error {
				return CleanupContiguousUploaded(tx, tt.safeGCIndex)
			})
			if err != nil {
				t.Fatalf("cleanup: %v", err)
			}

			var remaining []uint32
			err = db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket(bucketSegments)
				if b == nil {
					return nil
				}
				c := b.Cursor()
				for k, _ := c.First(); k != nil; k, _ = c.Next() {
					remaining = append(remaining, DecodeUint32(k))
				}
				return nil
			})
			if err != nil {
				t.Fatalf("verify: %v", err)
			}

			if len(remaining) != len(tt.wantRemaining) {
				t.Errorf("remaining segments = %v, want %v", remaining, tt.wantRemaining)
				return
			}
			for i, segID := range remaining {
				if segID != tt.wantRemaining[i] {
					t.Errorf("remaining[%d] = %d, want %d", i, segID, tt.wantRemaining[i])
				}
			}
		})
	}
}

func TestGetSealedSegmentsForNode_Ordering(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "fsm.db")

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	f, err := New(Config{
		DBPath: dbPath,
		NodeID: "node-1",
		Logger: logger,
	})
	if err != nil {
		t.Fatalf("New FSM: %v", err)
	}
	defer f.Close()

	segmentIDs := []uint32{5, 2, 8, 1, 10, 3}

	for _, segID := range segmentIDs {
		record := &SegmentRecord{
			State:      StateSealed,
			AssignedTo: "node-1",
			FirstIndex: uint64(segID * 100),
			LastIndex:  uint64(segID*100 + 99),
		}
		err := f.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketSegments)
			return b.Put(EncodeUint32(segID), record.Encode())
		})
		if err != nil {
			t.Fatalf("insert segment %d: %v", segID, err)
		}
	}

	sealed, err := f.GetSealedSegmentsForNode("node-1")
	if err != nil {
		t.Fatalf("GetSealedSegmentsForNode: %v", err)
	}

	if len(sealed) != len(segmentIDs) {
		t.Fatalf("expected %d segments, got %d", len(segmentIDs), len(sealed))
	}

	expected := []uint32{1, 2, 3, 5, 8, 10}
	for i, segID := range sealed {
		if segID != expected[i] {
			t.Errorf("position %d: got %d, want %d", i, segID, expected[i])
		}
	}

	for i := 1; i < len(sealed); i++ {
		if sealed[i] <= sealed[i-1] {
			t.Errorf("segments not in ascending order: %v", sealed)
			break
		}
	}
}

func TestGetSealedSegmentsForNode_FiltersByNodeAndState(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "fsm.db")

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	f, err := New(Config{
		DBPath: dbPath,
		NodeID: "node-1",
		Logger: logger,
	})
	if err != nil {
		t.Fatalf("New FSM: %v", err)
	}
	defer f.Close()

	segments := []struct {
		id         uint32
		state      uint8
		assignedTo string
	}{
		{1, StateSealed, "node-1"},
		{2, StateSealed, "node-2"},
		{3, StateUploaded, "node-1"},
		{4, StateSealed, "node-1"},
		{5, StateFailed, "node-1"},
		{6, StateSealed, "node-1"},
		{7, StateSealed, "node-3"},
	}

	for _, seg := range segments {
		record := &SegmentRecord{
			State:      seg.state,
			AssignedTo: seg.assignedTo,
			FirstIndex: uint64(seg.id * 100),
			LastIndex:  uint64(seg.id*100 + 99),
		}
		err := f.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketSegments)
			return b.Put(EncodeUint32(seg.id), record.Encode())
		})
		if err != nil {
			t.Fatalf("insert segment %d: %v", seg.id, err)
		}
	}

	sealed1, err := f.GetSealedSegmentsForNode("node-1")
	if err != nil {
		t.Fatalf("GetSealedSegmentsForNode(node-1): %v", err)
	}
	expected1 := []uint32{1, 4, 6}
	if len(sealed1) != len(expected1) {
		t.Errorf("node-1: expected %d segments, got %d: %v", len(expected1), len(sealed1), sealed1)
	}
	for i, segID := range sealed1 {
		if segID != expected1[i] {
			t.Errorf("node-1 position %d: got %d, want %d", i, segID, expected1[i])
		}
	}

	sealed2, err := f.GetSealedSegmentsForNode("node-2")
	if err != nil {
		t.Fatalf("GetSealedSegmentsForNode(node-2): %v", err)
	}
	expected2 := []uint32{2}
	if len(sealed2) != len(expected2) {
		t.Errorf("node-2: expected %d segments, got %d: %v", len(expected2), len(sealed2), sealed2)
	}

	sealed3, err := f.GetSealedSegmentsForNode("node-3")
	if err != nil {
		t.Fatalf("GetSealedSegmentsForNode(node-3): %v", err)
	}
	expected3 := []uint32{7}
	if len(sealed3) != len(expected3) {
		t.Errorf("node-3: expected %d segments, got %d: %v", len(expected3), len(sealed3), sealed3)
	}

	sealed4, err := f.GetSealedSegmentsForNode("node-99")
	if err != nil {
		t.Fatalf("GetSealedSegmentsForNode(node-99): %v", err)
	}
	if len(sealed4) != 0 {
		t.Errorf("node-99: expected 0 segments, got %d: %v", len(sealed4), sealed4)
	}
}

func TestGetSealedSegmentsForNode_LargeGaps(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "fsm.db")

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	f, err := New(Config{
		DBPath: dbPath,
		NodeID: "node-0",
		Logger: logger,
	})
	if err != nil {
		t.Fatalf("New FSM: %v", err)
	}
	defer f.Close()

	segmentIDs := []uint32{0, 3, 6, 9, 12, 15, 18, 21, 24, 27}

	for _, segID := range segmentIDs {
		record := &SegmentRecord{
			State:      StateSealed,
			AssignedTo: "node-0",
			FirstIndex: uint64(segID * 1000),
			LastIndex:  uint64(segID*1000 + 999),
		}
		err := f.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketSegments)
			return b.Put(EncodeUint32(segID), record.Encode())
		})
		if err != nil {
			t.Fatalf("insert segment %d: %v", segID, err)
		}
	}

	sealed, err := f.GetSealedSegmentsForNode("node-0")
	if err != nil {
		t.Fatalf("GetSealedSegmentsForNode: %v", err)
	}

	for i, segID := range sealed {
		if segID != segmentIDs[i] {
			t.Errorf("position %d: got %d, want %d", i, segID, segmentIDs[i])
		}
	}

	for i := 1; i < len(sealed); i++ {
		if sealed[i] <= sealed[i-1] {
			t.Errorf("segments not in ascending order at position %d: %v", i, sealed)
			break
		}
	}
}
