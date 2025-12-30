package command

import (
	"encoding/binary"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	raftfb "github.com/unijord/unijord/pkg/gen/go/fb/raft"
)

const (
	oneKB = 1024
	oneMB = oneKB * 1024
)

// Builder CommandBuilder helps construct Raft commands as FlatBuffers.
type Builder struct {
	pool    sync.Pool
	bufPool sync.Pool
}

// NewCommandBuilder creates a new command builder.
func NewCommandBuilder() *Builder {
	return &Builder{
		pool: sync.Pool{
			New: func() interface{} {
				return flatbuffers.NewBuilder(oneKB)
			},
		},
	}
}

func (cb *Builder) getBuilder() *flatbuffers.Builder {
	return cb.pool.Get().(*flatbuffers.Builder)
}

func (cb *Builder) putBuilder(b *flatbuffers.Builder) {
	b.Reset()
	cb.pool.Put(b)
}

// getBuf gets a byte buffer from the pool, resizing if needed.
func (cb *Builder) getBuf(size int) []byte {
	if v := cb.bufPool.Get(); v != nil {
		buf := v.([]byte)
		if cap(buf) >= size {
			return buf[:size]
		}
	}
	return make([]byte, size)
}

// putBuf returns a byte buffer to the pool.
func (cb *Builder) putBuf(buf []byte) {
	if cap(buf) <= oneMB {
		cb.bufPool.Put(buf[:0])
	}
}

// BuildSegmentSealed creates a SEGMENT_SEALED command.
func (cb *Builder) BuildSegmentSealed(
	segmentID uint32,
	firstIndex, lastIndex uint64,
	byteSize, entryCount uint64,
	assignedTo string,
) []byte {
	builder := cb.getBuilder()
	defer cb.putBuilder(builder)

	assignedToOffset := builder.CreateString(assignedTo)

	raftfb.SegmentSealedCommandStart(builder)
	raftfb.SegmentSealedCommandAddSegmentId(builder, segmentID)
	raftfb.SegmentSealedCommandAddFirstIndex(builder, firstIndex)
	raftfb.SegmentSealedCommandAddLastIndex(builder, lastIndex)
	raftfb.SegmentSealedCommandAddByteSize(builder, byteSize)
	raftfb.SegmentSealedCommandAddEntryCount(builder, entryCount)
	raftfb.SegmentSealedCommandAddSealedAt(builder, uint64(time.Now().UnixMicro()))
	raftfb.SegmentSealedCommandAddAssignedTo(builder, assignedToOffset)
	sealedCmd := raftfb.SegmentSealedCommandEnd(builder)

	raftfb.RaftCommandStart(builder)
	raftfb.RaftCommandAddType(builder, raftfb.CommandTypeSEGMENT_SEALED)
	raftfb.RaftCommandAddPayloadType(builder, raftfb.CommandPayloadSegmentSealedCommand)
	raftfb.RaftCommandAddPayload(builder, sealedCmd)
	cmd := raftfb.RaftCommandEnd(builder)

	builder.Finish(cmd)
	data := builder.FinishedBytes()
	result := make([]byte, len(data))
	copy(result, data)
	return result
}

// ParquetFile represents a Parquet file to be uploaded.
type ParquetFile struct {
	SchemaID  uint32
	ObjectKey string
	SizeBytes uint64
	RowCount  uint64
	Checksum  string
}

// BuildSegmentUploaded creates a SEGMENT_UPLOADED command.
func (cb *Builder) BuildSegmentUploaded(
	segmentID uint32,
	bucket string,
	files []ParquetFile,
	totalRows uint64,
	uploadedBy string,
) []byte {
	builder := cb.getBuilder()
	defer cb.putBuilder(builder)

	bucketOffset := builder.CreateString(bucket)
	uploadedByOffset := builder.CreateString(uploadedBy)

	fileOffsets := make([]flatbuffers.UOffsetT, len(files))
	for i, f := range files {
		keyOffset := builder.CreateString(f.ObjectKey)
		checksumOffset := builder.CreateString(f.Checksum)

		raftfb.ParquetFileInfoStart(builder)
		raftfb.ParquetFileInfoAddSchemaId(builder, f.SchemaID)
		raftfb.ParquetFileInfoAddObjectKey(builder, keyOffset)
		raftfb.ParquetFileInfoAddSizeBytes(builder, f.SizeBytes)
		raftfb.ParquetFileInfoAddRowCount(builder, f.RowCount)
		raftfb.ParquetFileInfoAddChecksum(builder, checksumOffset)
		fileOffsets[i] = raftfb.ParquetFileInfoEnd(builder)
	}

	raftfb.SegmentUploadedCommandStartFilesVector(builder, len(fileOffsets))
	for i := len(fileOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(fileOffsets[i])
	}
	filesVec := builder.EndVector(len(fileOffsets))

	raftfb.SegmentUploadedCommandStart(builder)
	raftfb.SegmentUploadedCommandAddSegmentId(builder, segmentID)
	raftfb.SegmentUploadedCommandAddObjectBucket(builder, bucketOffset)
	raftfb.SegmentUploadedCommandAddFiles(builder, filesVec)
	raftfb.SegmentUploadedCommandAddTotalRows(builder, totalRows)
	raftfb.SegmentUploadedCommandAddUploadedAt(builder, uint64(time.Now().UnixMicro()))
	raftfb.SegmentUploadedCommandAddUploadedBy(builder, uploadedByOffset)
	uploadedCmd := raftfb.SegmentUploadedCommandEnd(builder)

	raftfb.RaftCommandStart(builder)
	raftfb.RaftCommandAddType(builder, raftfb.CommandTypeSEGMENT_UPLOADED)
	raftfb.RaftCommandAddPayloadType(builder, raftfb.CommandPayloadSegmentUploadedCommand)
	raftfb.RaftCommandAddPayload(builder, uploadedCmd)
	cmd := raftfb.RaftCommandEnd(builder)

	builder.Finish(cmd)
	data := builder.FinishedBytes()
	result := make([]byte, len(data))
	copy(result, data)
	return result
}

// BuildAppend creates an APPEND command.
// Format: [4-byte len][event1][4-byte len][event2]...
func (cb *Builder) BuildAppend(events [][]byte) []byte {
	builder := cb.getBuilder()
	defer cb.putBuilder(builder)

	totalSize := 0
	for _, e := range events {
		totalSize += 4 + len(e)
	}

	batchData := cb.getBuf(totalSize)
	defer cb.putBuf(batchData)

	offset := 0
	for _, e := range events {
		binary.LittleEndian.PutUint32(batchData[offset:], uint32(len(e)))
		offset += 4
		copy(batchData[offset:], e)
		offset += len(e)
	}

	batchDataOffset := builder.CreateByteVector(batchData)
	raftfb.RaftCommandStart(builder)
	raftfb.RaftCommandAddType(builder, raftfb.CommandTypeAPPEND)
	raftfb.RaftCommandAddBatchData(builder, batchDataOffset)
	raftfb.RaftCommandAddBatchSize(builder, uint32(len(events)))
	cmd := raftfb.RaftCommandEnd(builder)

	builder.Finish(cmd)
	finished := builder.FinishedBytes()
	result := make([]byte, len(finished))
	copy(result, finished)
	return result
}

// BuildSegmentReassigned creates a SEGMENT_REASSIGNED command.
func (cb *Builder) BuildSegmentReassigned(
	segmentID uint32,
	previousNode, newNode string,
	reason string,
) []byte {
	builder := cb.getBuilder()
	defer cb.putBuilder(builder)

	previousNodeOffset := builder.CreateString(previousNode)
	newNodeOffset := builder.CreateString(newNode)
	reasonOffset := builder.CreateString(reason)

	raftfb.SegmentReassignedCommandStart(builder)
	raftfb.SegmentReassignedCommandAddSegmentId(builder, segmentID)
	raftfb.SegmentReassignedCommandAddPreviousNode(builder, previousNodeOffset)
	raftfb.SegmentReassignedCommandAddNewNode(builder, newNodeOffset)
	raftfb.SegmentReassignedCommandAddReassignedAt(builder, uint64(time.Now().UnixMicro()))
	raftfb.SegmentReassignedCommandAddReason(builder, reasonOffset)
	reassignedCmd := raftfb.SegmentReassignedCommandEnd(builder)

	raftfb.RaftCommandStart(builder)
	raftfb.RaftCommandAddType(builder, raftfb.CommandTypeSEGMENT_REASSIGNED)
	raftfb.RaftCommandAddPayloadType(builder, raftfb.CommandPayloadSegmentReassignedCommand)
	raftfb.RaftCommandAddPayload(builder, reassignedCmd)
	cmd := raftfb.RaftCommandEnd(builder)

	builder.Finish(cmd)
	data := builder.FinishedBytes()
	result := make([]byte, len(data))
	copy(result, data)
	return result
}

// BuildSegmentFailed creates a SEGMENT_FAILED command.
func (cb *Builder) BuildSegmentFailed(
	segmentID uint32,
	reason string,
	attemptCount uint32,
	lastAssignedTo string,
) []byte {
	builder := cb.getBuilder()
	defer cb.putBuilder(builder)

	reasonOffset := builder.CreateString(reason)
	lastAssignedToOffset := builder.CreateString(lastAssignedTo)

	raftfb.SegmentFailedCommandStart(builder)
	raftfb.SegmentFailedCommandAddSegmentId(builder, segmentID)
	raftfb.SegmentFailedCommandAddFailedAt(builder, uint64(time.Now().UnixMicro()))
	raftfb.SegmentFailedCommandAddReason(builder, reasonOffset)
	raftfb.SegmentFailedCommandAddAttemptCount(builder, attemptCount)
	raftfb.SegmentFailedCommandAddLastAssignedTo(builder, lastAssignedToOffset)
	failedCmd := raftfb.SegmentFailedCommandEnd(builder)

	raftfb.RaftCommandStart(builder)
	raftfb.RaftCommandAddType(builder, raftfb.CommandTypeSEGMENT_FAILED)
	raftfb.RaftCommandAddPayloadType(builder, raftfb.CommandPayloadSegmentFailedCommand)
	raftfb.RaftCommandAddPayload(builder, failedCmd)
	cmd := raftfb.RaftCommandEnd(builder)

	builder.Finish(cmd)
	data := builder.FinishedBytes()
	result := make([]byte, len(data))
	copy(result, data)
	return result
}

// DecodeBatchData decodes length-prefixed batch data into individual events.
func DecodeBatchData(batchData []byte, batchSize uint32) [][]byte {
	events := make([][]byte, 0, batchSize)
	offset := 0
	for offset < len(batchData) && uint32(len(events)) < batchSize {
		if offset+4 > len(batchData) {
			break
		}
		length := int(binary.LittleEndian.Uint32(batchData[offset:]))
		offset += 4

		if offset+length > len(batchData) {
			break
		}
		events = append(events, batchData[offset:offset+length])
		offset += length
	}
	return events
}
