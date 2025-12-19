package command

import (
	"bytes"
	"sync"
	"testing"

	raftfb "github.com/unijord/unijord/pkg/gen/go/fb/raft"
)

func TestFlatBufferPoolReset(t *testing.T) {
	builder := NewCommandBuilder()

	events1 := [][]byte{[]byte("first-event-data")}
	result1 := builder.BuildAppend(events1)

	events2 := [][]byte{[]byte("second-completely-different")}
	result2 := builder.BuildAppend(events2)

	if bytes.Equal(result1, result2) {
		t.Error("Results should be different - pool may not be resetting properly")
	}

	cmd1 := raftfb.GetRootAsRaftCommand(result1, 0)
	cmd2 := raftfb.GetRootAsRaftCommand(result2, 0)

	if cmd1.BatchSize() != 1 {
		t.Errorf("cmd1 batch size: expected 1, got %d", cmd1.BatchSize())
	}
	if cmd2.BatchSize() != 1 {
		t.Errorf("cmd2 batch size: expected 1, got %d", cmd2.BatchSize())
	}

	decoded1 := DecodeBatchData(cmd1.BatchDataBytes(), cmd1.BatchSize())
	decoded2 := DecodeBatchData(cmd2.BatchDataBytes(), cmd2.BatchSize())

	if !bytes.Equal(decoded1[0], events1[0]) {
		t.Errorf("decoded1 mismatch: expected %q, got %q", events1[0], decoded1[0])
	}
	if !bytes.Equal(decoded2[0], events2[0]) {
		t.Errorf("decoded2 mismatch: expected %q, got %q", events2[0], decoded2[0])
	}
}

func TestBufferPoolNoDataLeak(t *testing.T) {
	builder := NewCommandBuilder()

	sensitiveData := []byte("SECRET-PASSWORD-12345")
	events1 := [][]byte{sensitiveData}
	result1 := builder.BuildAppend(events1)

	cmd1 := raftfb.GetRootAsRaftCommand(result1, 0)
	decoded1 := DecodeBatchData(cmd1.BatchDataBytes(), cmd1.BatchSize())
	if !bytes.Equal(decoded1[0], sensitiveData) {
		t.Fatalf("first decode failed")
	}

	publicData := []byte("PUBLIC")
	events2 := [][]byte{publicData}
	result2 := builder.BuildAppend(events2)

	cmd2 := raftfb.GetRootAsRaftCommand(result2, 0)
	decoded2 := DecodeBatchData(cmd2.BatchDataBytes(), cmd2.BatchSize())

	if !bytes.Equal(decoded2[0], publicData) {
		t.Errorf("second decode mismatch: expected %q, got %q", publicData, decoded2[0])
	}

	// The batch data should NOT contain the sensitive data
	if bytes.Contains(cmd2.BatchDataBytes(), sensitiveData) {
		t.Error("SECURITY: sensitive data leaked into second result!")
	}
}

func TestPoolConcurrentSafety(t *testing.T) {
	builder := NewCommandBuilder()
	const goroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	errors := make(chan error, goroutines*iterations)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				data := []byte{byte(id), byte(i), byte(id >> 8), byte(i >> 8)}
				events := [][]byte{data}

				result := builder.BuildAppend(events)

				cmd := raftfb.GetRootAsRaftCommand(result, 0)
				if cmd.BatchSize() != 1 {
					errors <- errBatchSize(id, i, cmd.BatchSize())
					continue
				}

				decoded := DecodeBatchData(cmd.BatchDataBytes(), cmd.BatchSize())
				if len(decoded) != 1 {
					errors <- errDecodeLen(id, i, len(decoded))
					continue
				}

				if !bytes.Equal(decoded[0], data) {
					errors <- errDataMismatch(id, i, data, decoded[0])
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

func errBatchSize(g, i int, got uint32) error {
	return &poolError{msg: "batch size", g: g, i: i, expected: "1", got: string(rune(got))}
}

func errDecodeLen(g, i int, got int) error {
	return &poolError{msg: "decode len", g: g, i: i, expected: "1", got: string(rune(got))}
}

func errDataMismatch(g, i int, expected, got []byte) error {
	return &poolError{msg: "data mismatch", g: g, i: i, expected: string(expected), got: string(got)}
}

type poolError struct {
	msg      string
	g, i     int
	expected string
	got      string
}

func (e *poolError) Error() string {
	return e.msg
}

func TestPoolReturnsCorrectType(t *testing.T) {
	builder := NewCommandBuilder()

	for i := 0; i < 1000; i++ {
		events := [][]byte{[]byte("test")}
		result := builder.BuildAppend(events)
		if len(result) == 0 {
			t.Fatalf("iteration %d: empty result", i)
		}
	}
}

func TestBufPoolSizeHandling(t *testing.T) {
	builder := NewCommandBuilder()

	small := [][]byte{[]byte("x")}
	result1 := builder.BuildAppend(small)

	large := [][]byte{make([]byte, 10000)}
	result2 := builder.BuildAppend(large)

	result3 := builder.BuildAppend(small)

	for i, result := range [][]byte{result1, result2, result3} {
		cmd := raftfb.GetRootAsRaftCommand(result, 0)
		if cmd.BatchSize() != 1 {
			t.Errorf("result %d: expected batch size 1, got %d", i, cmd.BatchSize())
		}
	}
}

func TestBufPoolMaxSizeLimit(t *testing.T) {
	builder := NewCommandBuilder()

	huge := [][]byte{make([]byte, 2*1024*1024)} // 2MB
	result := builder.BuildAppend(huge)

	cmd := raftfb.GetRootAsRaftCommand(result, 0)
	if cmd.BatchSize() != 1 {
		t.Errorf("expected batch size 1, got %d", cmd.BatchSize())
	}
}
