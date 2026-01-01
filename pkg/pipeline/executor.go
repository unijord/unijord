package pipeline

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// Executor executes a compiled pipeline on events.
type Executor struct {
	pipeline *CompiledPipeline
}

// NewExecutor creates an executor for a compiled pipeline.
func NewExecutor(pipeline *CompiledPipeline) *Executor {
	return &Executor{pipeline: pipeline}
}

// Process executes the pipeline on a single event.
// The event should be a map[string]any representing the deserialized event.
func (e *Executor) Process(event map[string]any) Result {
	activation := map[string]any{"event": event}
	return e.processWithActivation(activation)
}

// ProcessWithEnvelope executes the pipeline on an event with envelope metadata.
// The envelope contains additional metadata like _event_id, _event_type, etc.
func (e *Executor) ProcessWithEnvelope(event map[string]any, envelope map[string]any) Result {
	// Build CEL activation with event and envelope variables
	activation := make(map[string]any, len(envelope)+1)
	activation["event"] = event

	// envelope fields
	for k, v := range envelope {
		activation[k] = v
	}
	
	return e.processWithActivation(activation)
}

// processWithActivation processes an event with a pre-built activation map.
func (e *Executor) processWithActivation(activation map[string]any) Result {
	// 1. Validate
	for _, v := range e.pipeline.Validations {
		out, _, err := v.Program.Eval(activation)
		if err != nil {
			return Result{
				Status:   StatusError,
				Error:    err,
				Location: fmt.Sprintf("validate[%d]", v.Index),
			}
		}

		val, ok := out.Value().(bool)
		if !ok {
			return Result{
				Status:   StatusError,
				Error:    fmt.Errorf("validation returned %T, expected bool", out.Value()),
				Location: fmt.Sprintf("validate[%d]", v.Index),
			}
		}

		if !val {
			return Result{
				Status: StatusRejected,
				Error: &ValidationError{
					Index:      v.Index,
					Expression: v.Source,
					Message:    "validation returned false",
				},
				Location: fmt.Sprintf("validate[%d]", v.Index),
			}
		}
	}

	// 2. Filter
	if e.pipeline.Filter != nil {
		out, _, err := e.pipeline.Filter.Program.Eval(activation)
		if err != nil {
			return Result{
				Status:   StatusError,
				Error:    &FilterError{Expression: e.pipeline.Filter.Source, Err: err},
				Location: "filter",
			}
		}

		val, ok := out.Value().(bool)
		if !ok {
			return Result{
				Status:   StatusError,
				Error:    fmt.Errorf("filter returned %T, expected bool", out.Value()),
				Location: "filter",
			}
		}

		if !val {
			return Result{Status: StatusFiltered}
		}
	}

	// 3. Transform (evaluate columns)
	row := make([]any, len(e.pipeline.Columns))
	for i, col := range e.pipeline.Columns {
		out, _, err := col.Program.Eval(activation)
		if err != nil {
			return Result{
				Status: StatusError,
				Error: &ColumnError{
					Name:       col.Name,
					Index:      col.Index,
					Expression: col.Source,
					Err:        err,
				},
				Location: fmt.Sprintf("columns[%d]", col.Index),
			}
		}
		row[i] = out.Value()
	}

	return Result{
		Status: StatusOK,
		Row:    row,
	}
}

// ProcessBatch executes the pipeline on multiple events.
// Returns results in the same order as input events.
func (e *Executor) ProcessBatch(events []map[string]any) *BatchResult {
	result := NewBatchResult(len(events))
	for _, event := range events {
		result.Add(e.Process(event))
	}
	return result
}

// Pipeline returns the compiled pipeline.
func (e *Executor) Pipeline() *CompiledPipeline {
	return e.pipeline
}

// OutputSchema returns the Arrow output schema.
func (e *Executor) OutputSchema() *arrow.Schema {
	return e.pipeline.OutputSchema
}

// ColumnNames returns the output column names.
func (e *Executor) ColumnNames() []string {
	return e.pipeline.ColumnNames()
}

// ColumnCount returns the number of output columns.
func (e *Executor) ColumnCount() int {
	return e.pipeline.ColumnCount()
}
