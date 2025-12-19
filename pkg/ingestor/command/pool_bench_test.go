package command

import (
	"encoding/binary"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	raftfb "github.com/unijord/unijord/pkg/gen/go/fb/raft"
)

func BuildAppendNoPool(events [][]byte) []byte {
	builder := flatbuffers.NewBuilder(1024)

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

func BenchmarkBuildAppend_WithPool(b *testing.B) {
	builder := NewCommandBuilder()
	events := make([][]byte, 100)
	for i := range events {
		events[i] = make([]byte, 512)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = builder.BuildAppend(events)
	}
}

func BenchmarkBuildAppend_NoPool(b *testing.B) {
	events := make([][]byte, 100)
	for i := range events {
		events[i] = make([]byte, 512)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = BuildAppendNoPool(events)
	}
}

func BenchmarkBuildAppend_WithPool_Parallel(b *testing.B) {
	builder := NewCommandBuilder()
	events := make([][]byte, 100)
	for i := range events {
		events[i] = make([]byte, 512)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = builder.BuildAppend(events)
		}
	})
}

func BenchmarkBuildAppend_NoPool_Parallel(b *testing.B) {
	events := make([][]byte, 100)
	for i := range events {
		events[i] = make([]byte, 512)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = BuildAppendNoPool(events)
		}
	})
}

func BenchmarkBuildAppend_BatchSizes(b *testing.B) {
	batchSizes := []int{10, 50, 100, 500, 1000}
	payloadSize := 256

	for _, batchSize := range batchSizes {
		events := make([][]byte, batchSize)
		for i := range events {
			events[i] = make([]byte, payloadSize)
		}

		b.Run("WithPool_"+string(rune('0'+batchSize/100)), func(b *testing.B) {
			builder := NewCommandBuilder()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = builder.BuildAppend(events)
			}
		})

		b.Run("NoPool_"+string(rune('0'+batchSize/100)), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = BuildAppendNoPool(events)
			}
		})
	}
}
