// Package fsm manages segment lifecycle state for the ingestor.
//
// The FSM is replicated across all nodes via Raft. It tracks metadata only -
// which segments exist, their state, who processes them, upload locations.
// The actual event data lives in the LogStore.
//
// # Segment states
//
// Segments move through these states:
//
//	SEALED  ---> UPLOADED
//	   |
//	   +-------> FAILED
//
// SEALED means the segment is closed for writes and ready for processing.
// UPLOADED means the Parquet files are in object storage. FAILED means
// something unrecoverable happened (corrupt data, schema mismatch) and
// manual intervention is needed.
//
// # Segment assignment
//
// Each sealed segment is assigned to one node for processing:
//
//	node = clusterNodes[segmentID % len(clusterNodes)]
//
// Round-robin by segment ID. Every node computes this locally, no
// coordination required. On restart, a node knows which segments it owns.
//
// # Flow
//
//  1. Events arrive, get batched, written to WAL
//  2. Batch hits size threshold, appender seals segment 5
//  3. SegmentSealedCommand through Raft
//  4. FSM creates record: {ID:5, State:SEALED, AssignedNode:"node-2"}
//  5. Callback fires, node-2 picks up the segment
//  6. Processor reads entries from WAL
//  7. Transform, partition, write Parquet, upload
//  8. SegmentUploadedCommand through Raft
//  9. FSM updates state to UPLOADED with file paths
//  10. If segments 0-4 are UPLOADED, safeGCIndex advances
//  11. WAL truncation removes old entries
package fsm
