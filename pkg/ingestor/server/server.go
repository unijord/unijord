package server

import (
	"context"
	"errors"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	ingestfb "github.com/unijord/unijord/pkg/gen/go/fb/ingest"
	ingestv1 "github.com/unijord/unijord/pkg/gen/go/proto/ingest/v1"
)

// IngestServer implements the gRPC IngestService.
type IngestServer struct {
	ingestv1.UnimplementedIngestServiceServer

	appender    Appender
	builderPool *builderPool
}

// NewIngestServer creates a new gRPC ingest server.
func NewIngestServer(appender Appender) *IngestServer {
	return &IngestServer{
		appender:    appender,
		builderPool: newBuilderPool(),
	}
}

func (s *IngestServer) Append(ctx context.Context, req *ingestv1.AppendRequest) (*ingestv1.AppendResponse, error) {
	if len(req.Events) == 0 {
		return &ingestv1.AppendResponse{
			Success:       true,
			ErrorCode:     ingestv1.ErrorCode_ERROR_CODE_OK,
			IngestedCount: 0,
		}, nil
	}

	if !s.appender.IsLeader() {
		return &ingestv1.AppendResponse{
			Success:       false,
			ErrorCode:     ingestv1.ErrorCode_ERROR_CODE_NOT_LEADER,
			ErrorMessage:  "not the leader",
			LeaderAddress: s.appender.LeaderAddr(),
		}, nil
	}

	walEntries := make([][]byte, len(req.Events))
	for i, event := range req.Events {
		walEntries[i] = s.eventToWalEntry(event)
	}

	lsn, err := s.appender.AppendBatch(walEntries)
	if err != nil {
		if errors.Is(err, ErrNotLeader) {
			return &ingestv1.AppendResponse{
				Success:       false,
				ErrorCode:     ingestv1.ErrorCode_ERROR_CODE_NOT_LEADER,
				ErrorMessage:  "not the leader",
				LeaderAddress: s.appender.LeaderAddr(),
			}, nil
		}
		return &ingestv1.AppendResponse{
			Success:      false,
			ErrorCode:    ingestv1.ErrorCode_ERROR_CODE_INTERNAL,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &ingestv1.AppendResponse{
		Success:       true,
		ErrorCode:     ingestv1.ErrorCode_ERROR_CODE_OK,
		CommittedLsn:  lsn,
		IngestedCount: uint32(len(req.Events)),
	}, nil
}

// AppendStream handles streaming event ingestion.
func (s *IngestServer) AppendStream(stream ingestv1.IngestService_AppendStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		resp, err := s.Append(stream.Context(), req)
		if err != nil {
			return err
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// GetStatus returns the current appender status.
func (s *IngestServer) GetStatus(ctx context.Context, req *ingestv1.GetStatusRequest) (*ingestv1.GetStatusResponse, error) {
	stats := s.appender.Stats()

	var health ingestv1.HealthStatus
	switch stats.Health() {
	case "healthy":
		health = ingestv1.HealthStatus_HEALTH_STATUS_HEALTHY
	case "degraded":
		health = ingestv1.HealthStatus_HEALTH_STATUS_DEGRADED
	default:
		health = ingestv1.HealthStatus_HEALTH_STATUS_UNSPECIFIED
	}

	return &ingestv1.GetStatusResponse{
		NodeId:               stats.NodeID,
		IsLeader:             stats.IsLeader,
		LeaderAddress:        stats.LeaderAddr,
		ActiveSegmentId:      stats.ActiveSegmentID,
		ActiveSegmentBytes:   stats.ActiveSegmentBytes,
		ActiveSegmentEntries: stats.ActiveSegmentEntries,
		PendingSegments:      uint32(stats.PendingSegments),
		Health:               health,
	}, nil
}

func (s *IngestServer) GetLeader(ctx context.Context, req *ingestv1.GetLeaderRequest) (*ingestv1.GetLeaderResponse, error) {
	return &ingestv1.GetLeaderResponse{
		LeaderId:      "",
		LeaderAddress: s.appender.LeaderAddr(),
		IsSelf:        s.appender.IsLeader(),
	}, nil
}

// eventToWalEntry converts a proto Event to a FlatBuffer WalEntry.
func (s *IngestServer) eventToWalEntry(event *ingestv1.Event) []byte {
	builder := s.builderPool.Get()
	defer s.builderPool.Put(builder)

	eventIDOffset := builder.CreateByteVector(event.EventId)
	eventTypeOffset := builder.CreateString(event.EventType)
	payloadOffset := builder.CreateByteVector(event.Payload)

	occurredAt := event.OccurredAt
	if occurredAt == 0 {
		occurredAt = uint64(time.Now().UnixMicro())
	}

	ingestfb.WalEntryStart(builder)
	ingestfb.WalEntryAddEventId(builder, eventIDOffset)
	ingestfb.WalEntryAddEventType(builder, eventTypeOffset)
	ingestfb.WalEntryAddOccurredAt(builder, occurredAt)
	ingestfb.WalEntryAddPayload(builder, payloadOffset)
	ingestfb.WalEntryAddSchemaId(builder, event.SchemaId)
	walEntry := ingestfb.WalEntryEnd(builder)

	builder.Finish(walEntry)
	data := builder.FinishedBytes()
	result := make([]byte, len(data))
	copy(result, data)
	return result
}

type builderPool struct {
	pool sync.Pool
}

func newBuilderPool() *builderPool {
	return &builderPool{
		pool: sync.Pool{
			New: func() interface{} {
				return flatbuffers.NewBuilder(1024)
			},
		},
	}
}

func (p *builderPool) Get() *flatbuffers.Builder {
	return p.pool.Get().(*flatbuffers.Builder)
}

func (p *builderPool) Put(b *flatbuffers.Builder) {
	b.Reset()
	p.pool.Put(b)
}

var _ ingestv1.IngestServiceServer = (*IngestServer)(nil)
