package raftwalfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// Codec defines the interface for encoding and decoding raft log entries.
// Implementations must be safe for concurrent use.
type Codec interface {
	ID() uint64

	// Encode serializes a raft.Log into a byte slice.
	Encode(log *raft.Log) ([]byte, error)

	// Decode deserializes a byte slice into a raft.Log.
	// The input buffer may be retained by the returned Log (zero-copy),
	// so callers must not modify the buffer after calling Decode.
	Decode(data []byte) (raft.Log, error)
}

const (
	// CodecBinaryV1ID is the ID for the built-in binary codec.
	CodecBinaryV1ID uint64 = 1
)

// DataMutator is called during Encode to inject the Raft log.Index as LSN into log.Data.
type DataMutator interface {
	MutateLSN(data []byte, lsn uint64) error
}

// encodeBufferPool is a global pool for encode buffers.
var encodeBufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 4096)
		return &buf
	},
}

// BinaryCodecV1 is the default codec using efficient binary encoding.
// Format: term(8) | index(8) | type(1) | appendedAt(8) | dataLen(varint) | data | extLen(varint) | ext
//
// If DataMutator is set, it will be called during Encode to inject the
// Raft log.Index as LSN into log.Data before encoding.
type BinaryCodecV1 struct {
	// DataMutator is called during Encode to mutate LSN in log.Data.
	DataMutator DataMutator
}

// ID returns the codec identifier.
func (c BinaryCodecV1) ID() uint64 {
	return CodecBinaryV1ID
}

// Encode serializes a raft.Log into a pre-sized buffer.
// The returned buffer is from a pool. Call ReleaseEncodeBuffers after
// the buffers have been copied (e.g., after WAL write completes).
func (c BinaryCodecV1) Encode(l *raft.Log) ([]byte, error) {
	// Mutate LSN in data before encoding - only for LogCommand (user data)
	if c.DataMutator != nil && len(l.Data) > 0 && l.Type == raft.LogCommand {
		if err := c.DataMutator.MutateLSN(l.Data, l.Index); err != nil {
			return nil, fmt.Errorf("mutate LSN in data: %w", err)
		}
	}

	dataLenSize := varintSize(uint64(len(l.Data)))
	extLenSize := varintSize(uint64(len(l.Extensions)))
	totalSize := 8 + 8 + 1 + 8 + dataLenSize + len(l.Data) + extLenSize + len(l.Extensions)

	// buffer from pool or allocate new one
	var buf []byte
	if pooled := encodeBufferPool.Get().(*[]byte); cap(*pooled) >= totalSize {
		buf = (*pooled)[:totalSize]
	} else {
		// larger buffer, allocate new one
		buf = make([]byte, totalSize)
	}

	offset := 0

	// Term (8 bytes)
	binary.LittleEndian.PutUint64(buf[offset:], l.Term)
	offset += 8

	// Index (8 bytes)
	binary.LittleEndian.PutUint64(buf[offset:], l.Index)
	offset += 8

	// Type (1 byte)
	buf[offset] = byte(l.Type)
	offset++

	// AppendedAt (8 bytes)
	var appended int64
	if !l.AppendedAt.IsZero() {
		appended = l.AppendedAt.UnixNano()
	}
	binary.LittleEndian.PutUint64(buf[offset:], uint64(appended))
	offset += 8

	// Data length (varint) + data
	offset += binary.PutUvarint(buf[offset:], uint64(len(l.Data)))
	copy(buf[offset:], l.Data)
	offset += len(l.Data)

	// Extensions length (varint) + extensions
	offset += binary.PutUvarint(buf[offset:], uint64(len(l.Extensions)))
	copy(buf[offset:], l.Extensions)

	return buf, nil
}

// ReleaseEncodeBuffers returns encoded buffers to the pool.
// Call this after WAL write completes (data has been copied to mmap).
func ReleaseEncodeBuffers(buffers [][]byte) {
	for i := range buffers {
		if buffers[i] != nil && cap(buffers[i]) <= 64*1024 {
			buf := buffers[i][:0]
			encodeBufferPool.Put(&buf)
			buffers[i] = nil
		}
	}
}

// Decode deserializes a raft.Log.
func (c BinaryCodecV1) Decode(data []byte) (raft.Log, error) {
	const minSize = 8 + 8 + 1 + 8
	if len(data) < minSize {
		return raft.Log{}, errors.New("data too short")
	}

	var l raft.Log

	// Term (8 bytes)
	l.Term = binary.LittleEndian.Uint64(data[0:8])

	// Index (8 bytes)
	l.Index = binary.LittleEndian.Uint64(data[8:16])

	// Type (1 byte)
	l.Type = raft.LogType(data[16])

	// AppendedAt (8 bytes)
	ts := binary.LittleEndian.Uint64(data[17:25])
	if ts != 0 {
		l.AppendedAt = time.Unix(0, int64(ts))
	}

	offset := 25

	// Data length (varint) + data
	dataLen, n := binary.Uvarint(data[offset:])
	if n <= 0 {
		return raft.Log{}, errors.New("invalid data length varint")
	}
	offset += n

	if dataLen > 0 {
		if offset+int(dataLen) > len(data) {
			return raft.Log{}, errors.New("data length exceeds buffer")
		}
		l.Data = data[offset : offset+int(dataLen)]
		offset += int(dataLen)
	}

	// Extensions length (varint) + extensions
	extLen, n := binary.Uvarint(data[offset:])
	if n <= 0 {
		return raft.Log{}, errors.New("invalid extensions length varint")
	}
	offset += n

	if extLen > 0 {
		if offset+int(extLen) > len(data) {
			return raft.Log{}, errors.New("extensions length exceeds buffer")
		}
		l.Extensions = data[offset : offset+int(extLen)] // Zero-copy
	}

	return l, nil
}

var _ Codec = (*BinaryCodecV1)(nil)
