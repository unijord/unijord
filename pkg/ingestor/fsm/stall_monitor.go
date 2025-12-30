package fsm

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/unijord/unijord/pkg/ingestor/command"
)

// StallMonitorConfig configures the stall monitor.
type StallMonitorConfig struct {
	StallTimeout     time.Duration
	CheckInterval    time.Duration
	MaxReassignments int
	// NodeSelector is called to select a new node for reassignment.
	// If nil, uses round-robin across available nodes.
	NodeSelector func(excludeNode string) string
	Logger       *slog.Logger
}

// DefaultStallMonitorConfig returns sensible defaults.
func DefaultStallMonitorConfig() StallMonitorConfig {
	return StallMonitorConfig{
		StallTimeout:     5 * time.Minute,
		CheckInterval:    30 * time.Second,
		MaxReassignments: 3,
	}
}

// StallMonitor detects and handles stalled segment processing.
// It runs on the leader node and periodically checks for segments
// that have been in SEALED state for too long without progressing
// to UPLOADED. When a stall is detected, it reassigns the segment
// to a different node.
type StallMonitor struct {
	config StallMonitorConfig
	fsm    *FSM
	raft   RaftApplier

	cmdBuilder *command.Builder
	logger     *slog.Logger

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

type RaftApplier interface {
	Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture
	State() raft.RaftState
	GetConfiguration() raft.ConfigurationFuture
}

// NewStallMonitor creates a new stall monitor.
func NewStallMonitor(fsm *FSM, raft RaftApplier, config StallMonitorConfig) *StallMonitor {
	if config.StallTimeout == 0 {
		config.StallTimeout = 5 * time.Minute
	}
	if config.CheckInterval == 0 {
		config.CheckInterval = 30 * time.Second
	}
	if config.MaxReassignments == 0 {
		config.MaxReassignments = 3
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	return &StallMonitor{
		config:     config,
		fsm:        fsm,
		raft:       raft,
		cmdBuilder: command.NewCommandBuilder(),
		logger:     config.Logger.With("component", "stall-monitor"),
		stopCh:     make(chan struct{}),
	}
}

func (m *StallMonitor) Start() {
	m.wg.Add(1)
	go m.run()
}

func (m *StallMonitor) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
	m.wg.Wait()
}

func (m *StallMonitor) run() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkForStalls()
		case <-m.stopCh:
			return
		}
	}
}

func (m *StallMonitor) checkForStalls() {
	if m.raft.State() != raft.Leader {
		return
	}

	//et all SEALED segments
	sealedSegments, err := m.fsm.GetSealedSegments()
	if err != nil {
		m.logger.Error("failed to get sealed segments", "error", err)
		return
	}

	now := time.Now().UnixMicro()
	stallThreshold := uint64(now) - uint64(m.config.StallTimeout.Microseconds())

	for _, segmentID := range sealedSegments {
		record, err := m.fsm.GetSegment(segmentID)
		if err != nil {
			m.logger.Error("failed to get segment record",
				"segment_id", segmentID,
				"error", err)
			continue
		}

		if record == nil || record.State != StateSealed {
			continue
		}

		// if segment has been sealed for too long
		if record.SealedAt > stallThreshold {
			continue
		}

		m.handleStalledSegment(segmentID, record)
	}
}

func (m *StallMonitor) handleStalledSegment(segmentID uint32, record *SegmentRecord) {
	count := int(record.AttemptCount)

	if count >= m.config.MaxReassignments {
		m.logger.Error("segment exceeded max reassignments, marking as failed",
			"segment_id", segmentID,
			"reassignments", count,
			"max", m.config.MaxReassignments,
			"assigned_to", record.AssignedTo,
		)

		reason := "exceeded_max_reassignments"
		cmd := m.cmdBuilder.BuildSegmentFailed(
			segmentID,
			reason,
			record.AttemptCount,
			record.AssignedTo,
		)

		future := m.raft.Apply(cmd, 10*time.Second)
		if err := future.Error(); err != nil {
			m.logger.Error("failed to apply segment failed command",
				"segment_id", segmentID,
				"error", err,
			)
		}
		return
	}

	newNode := m.selectNewNode(record.AssignedTo)
	if newNode == "" || newNode == record.AssignedTo {
		m.logger.Warn("no alternative node available for reassignment",
			"segment_id", segmentID,
			"current_node", record.AssignedTo,
		)
		return
	}

	m.logger.Info("reassigning stalled segment",
		"segment_id", segmentID,
		"previous_node", record.AssignedTo,
		"new_node", newNode,
		"stalled_for", time.Since(time.UnixMicro(int64(record.SealedAt))),
		"attempt_count", count+1,
	)

	cmd := m.cmdBuilder.BuildSegmentReassigned(
		segmentID,
		record.AssignedTo,
		newNode,
		"stall_detected",
	)

	future := m.raft.Apply(cmd, 10*time.Second)
	if err := future.Error(); err != nil {
		m.logger.Error("failed to apply segment reassignment",
			"segment_id", segmentID,
			"error", err,
		)
	}
}

func (m *StallMonitor) selectNewNode(excludeNode string) string {
	if m.config.NodeSelector != nil {
		return m.config.NodeSelector(excludeNode)
	}

	configFuture := m.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		m.logger.Error("failed to get raft configuration", "error", err)
		return ""
	}

	config := configFuture.Configuration()
	servers := config.Servers

	var candidates []string
	for _, server := range servers {
		nodeID := string(server.ID)
		if nodeID != excludeNode && server.Suffrage == raft.Voter {
			candidates = append(candidates, nodeID)
		}
	}

	if len(candidates) == 0 {
		return ""
	}

	return candidates[0]
}

// GetReassignmentCount returns the current reassignment count for a segment
// by reading from the FSM's replicated state.
func (m *StallMonitor) GetReassignmentCount(segmentID uint32) int {
	record, err := m.fsm.GetSegment(segmentID)
	if err != nil || record == nil {
		return 0
	}
	return int(record.AttemptCount)
}

func (m *StallMonitor) ForceCheck(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	default:
		m.checkForStalls()
	}
}
