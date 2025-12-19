package server

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	ingestv1 "github.com/unijord/unijord/pkg/gen/go/proto/ingest/v1"
)

func TestAppend_EmptyBatch(t *testing.T) {
	mock := NewMockAppender()
	srv := NewIngestServer(mock)

	resp, err := srv.Append(context.Background(), &ingestv1.AppendRequest{
		Events: nil,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Error("expected success for empty batch")
	}
	if resp.IngestedCount != 0 {
		t.Errorf("expected 0 ingested, got %d", resp.IngestedCount)
	}
}

func TestAppend_NotLeader(t *testing.T) {
	mock := NewMockAppender()
	mock.SetLeader(false)
	mock.SetLeaderAddr("leader:9090")
	srv := NewIngestServer(mock)

	id := uuid.New()
	resp, err := srv.Append(context.Background(), &ingestv1.AppendRequest{
		Events: []*ingestv1.Event{
			{EventId: id[:], EventType: "test", Payload: []byte("test")},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Success {
		t.Error("expected failure when not leader")
	}
	if resp.ErrorCode != ingestv1.ErrorCode_ERROR_CODE_NOT_LEADER {
		t.Errorf("expected NOT_LEADER error, got %v", resp.ErrorCode)
	}
	if resp.LeaderAddress != "leader:9090" {
		t.Errorf("expected leader address, got %s", resp.LeaderAddress)
	}
}

func TestAppend_Success(t *testing.T) {
	mock := NewMockAppender()
	srv := NewIngestServer(mock)

	id1, id2 := uuid.New(), uuid.New()
	events := []*ingestv1.Event{
		{EventId: id1[:], EventType: "order.created", Payload: []byte(`{"id": 1}`)},
		{EventId: id2[:], EventType: "order.updated", Payload: []byte(`{"id": 1}`)},
	}

	resp, err := srv.Append(context.Background(), &ingestv1.AppendRequest{
		Events: events,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Error("expected success")
	}
	if resp.ErrorCode != ingestv1.ErrorCode_ERROR_CODE_OK {
		t.Errorf("expected OK error code, got %v", resp.ErrorCode)
	}
	if resp.CommittedLsn == 0 {
		t.Error("expected non-zero LSN")
	}
	if resp.IngestedCount != 2 {
		t.Errorf("expected 2 ingested, got %d", resp.IngestedCount)
	}

	entries := mock.GetEntries()
	if len(entries) != 2 {
		t.Errorf("expected 2 entries stored, got %d", len(entries))
	}
}

func TestAppend_AppendError(t *testing.T) {
	mock := NewMockAppender()
	mock.SetAppendError(errors.New("storage full"))
	srv := NewIngestServer(mock)

	id := uuid.New()
	resp, err := srv.Append(context.Background(), &ingestv1.AppendRequest{
		Events: []*ingestv1.Event{
			{EventId: id[:], EventType: "test", Payload: []byte("test")},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Success {
		t.Error("expected failure")
	}
	if resp.ErrorCode != ingestv1.ErrorCode_ERROR_CODE_INTERNAL {
		t.Errorf("expected INTERNAL error, got %v", resp.ErrorCode)
	}
	if resp.ErrorMessage != "storage full" {
		t.Errorf("expected error message, got %s", resp.ErrorMessage)
	}
}

func TestAppend_NotLeaderError(t *testing.T) {
	mock := NewMockAppender()
	mock.SetAppendError(ErrNotLeader)
	mock.SetLeaderAddr("new-leader:9090")
	srv := NewIngestServer(mock)

	id := uuid.New()
	resp, err := srv.Append(context.Background(), &ingestv1.AppendRequest{
		Events: []*ingestv1.Event{
			{EventId: id[:], EventType: "test", Payload: []byte("test")},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Success {
		t.Error("expected failure")
	}
	if resp.ErrorCode != ingestv1.ErrorCode_ERROR_CODE_NOT_LEADER {
		t.Errorf("expected NOT_LEADER error, got %v", resp.ErrorCode)
	}
	if resp.LeaderAddress != "new-leader:9090" {
		t.Errorf("expected leader address, got %s", resp.LeaderAddress)
	}
}

func TestGetStatus(t *testing.T) {
	mock := NewMockAppender()
	mock.SetStats(AppenderStats{
		NodeID:               "node-1",
		IsLeader:             true,
		LeaderAddr:           "",
		ActiveSegmentID:      5,
		ActiveSegmentBytes:   1024 * 1024,
		ActiveSegmentEntries: 1000,
		PendingSegments:      3,
	})
	srv := NewIngestServer(mock)

	resp, err := srv.GetStatus(context.Background(), &ingestv1.GetStatusRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.NodeId != "node-1" {
		t.Errorf("expected node-1, got %s", resp.NodeId)
	}
	if !resp.IsLeader {
		t.Error("expected IsLeader=true")
	}
	if resp.ActiveSegmentId != 5 {
		t.Errorf("expected segment 5, got %d", resp.ActiveSegmentId)
	}
	if resp.ActiveSegmentBytes != 1024*1024 {
		t.Errorf("expected 1MB, got %d", resp.ActiveSegmentBytes)
	}
	if resp.ActiveSegmentEntries != 1000 {
		t.Errorf("expected 1000 entries, got %d", resp.ActiveSegmentEntries)
	}
	if resp.PendingSegments != 3 {
		t.Errorf("expected 3 pending, got %d", resp.PendingSegments)
	}
	if resp.Health != ingestv1.HealthStatus_HEALTH_STATUS_HEALTHY {
		t.Errorf("expected healthy, got %v", resp.Health)
	}
}

func TestGetStatus_Degraded(t *testing.T) {
	mock := NewMockAppender()
	mock.SetStats(AppenderStats{
		NodeID:     "node-1",
		IsLeader:   false,
		LeaderAddr: "",
	})
	srv := NewIngestServer(mock)

	resp, err := srv.GetStatus(context.Background(), &ingestv1.GetStatusRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Health != ingestv1.HealthStatus_HEALTH_STATUS_DEGRADED {
		t.Errorf("expected degraded, got %v", resp.Health)
	}
}

func TestGetLeader(t *testing.T) {
	mock := NewMockAppender()
	mock.SetLeader(false)
	mock.SetLeaderAddr("leader:9090")
	srv := NewIngestServer(mock)

	resp, err := srv.GetLeader(context.Background(), &ingestv1.GetLeaderRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.LeaderAddress != "leader:9090" {
		t.Errorf("expected leader:9090, got %s", resp.LeaderAddress)
	}
	if resp.IsSelf {
		t.Error("expected IsSelf=false")
	}
}

func TestGetLeader_IsSelf(t *testing.T) {
	mock := NewMockAppender()
	mock.SetLeader(true)
	srv := NewIngestServer(mock)

	resp, err := srv.GetLeader(context.Background(), &ingestv1.GetLeaderRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.IsSelf {
		t.Error("expected IsSelf=true")
	}
}
