package command

import (
	"bytes"
	"encoding/binary"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	raftfb "github.com/unijord/unijord/pkg/gen/go/fb/raft"
)

func TestBuildAppend_SingleEvent(t *testing.T) {
	builder := NewCommandBuilder()
	event := []byte("hello world")
	result := builder.BuildAppend([][]byte{event})
	if len(result) == 0 {
		t.Fatal("expected non-empty result")
	}
}

func TestBuildAppend_MultipleEvents(t *testing.T) {
	builder := NewCommandBuilder()
	events := [][]byte{
		[]byte("event1"),
		[]byte("event2"),
		[]byte("event3"),
	}

	result := builder.BuildAppend(events)
	if len(result) == 0 {
		t.Fatal("expected non-empty result")
	}
}

func TestDecodeBatchData_SingleEvent(t *testing.T) {
	event := []byte("hello")
	batchData := make([]byte, 4+len(event))
	binary.LittleEndian.PutUint32(batchData[0:], uint32(len(event)))
	copy(batchData[4:], event)

	decoded := DecodeBatchData(batchData, 1)
	if len(decoded) != 1 {
		t.Fatalf("expected 1 event, got %d", len(decoded))
	}
	if !bytes.Equal(decoded[0], event) {
		t.Errorf("expected %q, got %q", event, decoded[0])
	}
}

func TestDecodeBatchData_MultipleEvents(t *testing.T) {
	events := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	totalSize := 0
	for _, e := range events {
		totalSize += 4 + len(e)
	}
	batchData := make([]byte, totalSize)

	offset := 0
	for _, e := range events {
		binary.LittleEndian.PutUint32(batchData[offset:], uint32(len(e)))
		offset += 4
		copy(batchData[offset:], e)
		offset += len(e)
	}

	decoded := DecodeBatchData(batchData, uint32(len(events)))
	if len(decoded) != len(events) {
		t.Fatalf("expected %d events, got %d", len(events), len(decoded))
	}
	for i, e := range events {
		if !bytes.Equal(decoded[i], e) {
			t.Errorf("event %d: expected %q, got %q", i, e, decoded[i])
		}
	}
}

func TestDecodeBatchData_LargeEvent(t *testing.T) {
	event := make([]byte, 300)
	for i := range event {
		event[i] = byte(i % 256)
	}

	batchData := make([]byte, 4+len(event))
	binary.LittleEndian.PutUint32(batchData[0:], uint32(len(event)))
	copy(batchData[4:], event)

	decoded := DecodeBatchData(batchData, 1)

	if len(decoded) != 1 {
		t.Fatalf("expected 1 event, got %d", len(decoded))
	}
	if len(decoded[0]) != 300 {
		t.Errorf("expected event length 300, got %d", len(decoded[0]))
	}
	if !bytes.Equal(decoded[0], event) {
		t.Error("decoded event doesn't match original")
	}
}

func TestDecodeBatchData_EmptyBatch(t *testing.T) {
	decoded := DecodeBatchData([]byte{}, 0)

	if len(decoded) != 0 {
		t.Errorf("expected 0 events, got %d", len(decoded))
	}
}

func TestDecodeBatchData_TruncatedLength(t *testing.T) {
	batchData := []byte{0x05, 0x00}

	decoded := DecodeBatchData(batchData, 1)

	if len(decoded) != 0 {
		t.Errorf("expected 0 events for truncated length, got %d", len(decoded))
	}
}

func TestDecodeBatchData_TruncatedData(t *testing.T) {
	batchData := []byte{
		0x0A, 0x00, 0x00, 0x00,
		0x01, 0x02, 0x03, 0x04, 0x05,
	}

	decoded := DecodeBatchData(batchData, 1)

	if len(decoded) != 0 {
		t.Errorf("expected 0 events for truncated data, got %d", len(decoded))
	}
}

func TestDecodeBatchData_BatchSizeLimit(t *testing.T) {
	events := [][]byte{
		[]byte("one"),
		[]byte("two"),
		[]byte("three"),
	}

	totalSize := 0
	for _, e := range events {
		totalSize += 4 + len(e)
	}
	batchData := make([]byte, totalSize)

	offset := 0
	for _, e := range events {
		batchData[offset] = byte(len(e))
		batchData[offset+1] = 0
		batchData[offset+2] = 0
		batchData[offset+3] = 0
		offset += 4
		copy(batchData[offset:], e)
		offset += len(e)
	}

	decoded := DecodeBatchData(batchData, 2)

	if len(decoded) != 2 {
		t.Fatalf("expected 2 events (limited by batchSize), got %d", len(decoded))
	}
	if !bytes.Equal(decoded[0], events[0]) {
		t.Errorf("event 0: expected %q, got %q", events[0], decoded[0])
	}
	if !bytes.Equal(decoded[1], events[1]) {
		t.Errorf("event 1: expected %q, got %q", events[1], decoded[1])
	}
}

func TestEncodeDecode_Roundtrip(t *testing.T) {
	builder := NewCommandBuilder()

	testCases := []struct {
		name   string
		events [][]byte
	}{
		{
			name:   "single small event",
			events: [][]byte{[]byte("hello")},
		},
		{
			name:   "multiple events",
			events: [][]byte{[]byte("one"), []byte("two"), []byte("three")},
		},
		{
			name:   "empty event",
			events: [][]byte{[]byte{}},
		},
		{
			name:   "mixed sizes",
			events: [][]byte{[]byte("a"), make([]byte, 1000), []byte("z")},
		},
		{
			name:   "binary data",
			events: [][]byte{{0x00, 0xFF, 0x7F, 0x80}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmdData := builder.BuildAppend(tc.events)

			totalSize := 0
			for _, e := range tc.events {
				totalSize += 4 + len(e)
			}
			batchData := make([]byte, totalSize)

			offset := 0
			for _, e := range tc.events {
				binary.LittleEndian.PutUint32(batchData[offset:], uint32(len(e)))
				offset += 4
				copy(batchData[offset:], e)
				offset += len(e)
			}

			decoded := DecodeBatchData(batchData, uint32(len(tc.events)))

			if len(decoded) != len(tc.events) {
				t.Fatalf("expected %d events, got %d", len(tc.events), len(decoded))
			}
			for i, e := range tc.events {
				if !bytes.Equal(decoded[i], e) {
					t.Errorf("event %d mismatch", i)
				}
			}

			if len(cmdData) == 0 {
				t.Error("BuildAppend produced empty result")
			}
		})
	}
}

func TestBuildSegmentSealed(t *testing.T) {
	builder := NewCommandBuilder()
	data := builder.BuildSegmentSealed(7, 10, 20, 100, 5, "node-2")
	cmd := raftfb.GetRootAsRaftCommand(data, 0)
	if cmd.Type() != raftfb.CommandTypeSEGMENT_SEALED {
		t.Fatalf("unexpected type %v", cmd.Type())
	}
	if cmd.PayloadType() != raftfb.CommandPayloadSegmentSealedCommand {
		t.Fatalf("unexpected payload type %v", cmd.PayloadType())
	}
	var tbl flatbuffers.Table
	if !cmd.Payload(&tbl) {
		t.Fatal("payload missing")
	}
	var sealed raftfb.SegmentSealedCommand
	sealed.Init(tbl.Bytes, tbl.Pos)
	if sealed.SegmentId() != 7 {
		t.Fatalf("segment id %d", sealed.SegmentId())
	}
	if sealed.FirstIndex() != 10 || sealed.LastIndex() != 20 {
		t.Fatalf("indexes %d %d", sealed.FirstIndex(), sealed.LastIndex())
	}
	if sealed.ByteSize() != 100 || sealed.EntryCount() != 5 {
		t.Fatalf("sizes %d %d", sealed.ByteSize(), sealed.EntryCount())
	}
	if string(sealed.AssignedTo()) != "node-2" {
		t.Fatalf("assigned %s", string(sealed.AssignedTo()))
	}
	if sealed.SealedAt() == 0 {
		t.Fatalf("sealed_at not set")
	}
}

func TestBuildSegmentUploaded(t *testing.T) {
	builder := NewCommandBuilder()
	files := []ParquetFile{
		{SchemaID: 1, ObjectKey: "obj-1", SizeBytes: 10, RowCount: 2, Checksum: "a"},
		{SchemaID: 2, ObjectKey: "obj-2", SizeBytes: 20, RowCount: 4, Checksum: "b"},
	}
	data := builder.BuildSegmentUploaded(9, "bucket-x", files, 6, "node-a")
	cmd := raftfb.GetRootAsRaftCommand(data, 0)
	if cmd.Type() != raftfb.CommandTypeSEGMENT_UPLOADED {
		t.Fatalf("unexpected type %v", cmd.Type())
	}
	if cmd.PayloadType() != raftfb.CommandPayloadSegmentUploadedCommand {
		t.Fatalf("unexpected payload type %v", cmd.PayloadType())
	}
	var tbl flatbuffers.Table
	if !cmd.Payload(&tbl) {
		t.Fatal("payload missing")
	}
	var uploaded raftfb.SegmentUploadedCommand
	uploaded.Init(tbl.Bytes, tbl.Pos)
	if uploaded.SegmentId() != 9 {
		t.Fatalf("segment id %d", uploaded.SegmentId())
	}
	if string(uploaded.ObjectBucket()) != "bucket-x" {
		t.Fatalf("bucket %s", string(uploaded.ObjectBucket()))
	}
	if uploaded.TotalRows() != 6 {
		t.Fatalf("total rows %d", uploaded.TotalRows())
	}
	if uploaded.UploadedAt() == 0 {
		t.Fatalf("uploaded_at not set")
	}
	if string(uploaded.UploadedBy()) != "node-a" {
		t.Fatalf("uploadedBy %s", string(uploaded.UploadedBy()))
	}
	if uploaded.FilesLength() != len(files) {
		t.Fatalf("files len %d", uploaded.FilesLength())
	}
	var f raftfb.ParquetFileInfo
	if !uploaded.Files(&f, 0) {
		t.Fatal("file 0 missing")
	}
	if f.SchemaId() != 1 || string(f.ObjectKey()) != "obj-1" || f.SizeBytes() != 10 || f.RowCount() != 2 || string(f.Checksum()) != "a" {
		t.Fatalf("file0 mismatch")
	}
	if !uploaded.Files(&f, 1) {
		t.Fatal("file 1 missing")
	}
	if f.SchemaId() != 2 || string(f.ObjectKey()) != "obj-2" || f.SizeBytes() != 20 || f.RowCount() != 4 || string(f.Checksum()) != "b" {
		t.Fatalf("file1 mismatch")
	}
}

func TestBuildSegmentReassigned(t *testing.T) {
	builder := NewCommandBuilder()
	data := builder.BuildSegmentReassigned(3, "node-1", "node-2", "rebalance")
	cmd := raftfb.GetRootAsRaftCommand(data, 0)
	if cmd.Type() != raftfb.CommandTypeSEGMENT_REASSIGNED {
		t.Fatalf("unexpected type %v", cmd.Type())
	}
	if cmd.PayloadType() != raftfb.CommandPayloadSegmentReassignedCommand {
		t.Fatalf("unexpected payload type %v", cmd.PayloadType())
	}
	var tbl flatbuffers.Table
	if !cmd.Payload(&tbl) {
		t.Fatal("payload missing")
	}
	var reassigned raftfb.SegmentReassignedCommand
	reassigned.Init(tbl.Bytes, tbl.Pos)
	if reassigned.SegmentId() != 3 {
		t.Fatalf("segment id %d", reassigned.SegmentId())
	}
	if string(reassigned.PreviousNode()) != "node-1" || string(reassigned.NewNode()) != "node-2" {
		t.Fatalf("nodes %s %s", string(reassigned.PreviousNode()), string(reassigned.NewNode()))
	}
	if string(reassigned.Reason()) != "rebalance" {
		t.Fatalf("reason %s", string(reassigned.Reason()))
	}
	if reassigned.ReassignedAt() == 0 {
		t.Fatalf("reassigned_at not set")
	}
}

func BenchmarkBuildAppend(b *testing.B) {
	builder := NewCommandBuilder()
	events := make([][]byte, 100)
	for i := range events {
		events[i] = make([]byte, 512)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = builder.BuildAppend(events)
	}
}

func BenchmarkDecodeBatchData(b *testing.B) {
	events := make([][]byte, 100)
	for i := range events {
		events[i] = make([]byte, 512)
	}

	totalSize := 0
	for _, e := range events {
		totalSize += 4 + len(e)
	}
	batchData := make([]byte, totalSize)

	offset := 0
	for _, e := range events {
		batchData[offset] = byte(len(e))
		batchData[offset+1] = byte(len(e) >> 8)
		batchData[offset+2] = byte(len(e) >> 16)
		batchData[offset+3] = byte(len(e) >> 24)
		offset += 4
		copy(batchData[offset:], e)
		offset += len(e)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DecodeBatchData(batchData, uint32(len(events)))
	}
}
