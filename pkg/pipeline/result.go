package pipeline

import (
	"errors"
	"fmt"
)

var (
	// ErrValidationFailed indicates an event failed validation.
	ErrValidationFailed = errors.New("validation failed")
)

type Status int

const (
	// StatusOK means event passed validation and filter.
	StatusOK Status = iota

	// StatusFiltered means event was dropped by filter (not an error).
	StatusFiltered

	// StatusRejected means event failed validation (error).
	StatusRejected

	// StatusError means an unexpected error occurred.
	StatusError
)

// String returns the string representation of Status.
func (s Status) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusFiltered:
		return "filtered"
	case StatusRejected:
		return "rejected"
	case StatusError:
		return "error"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// Result represents the outcome of processing a single event.
type Result struct {
	// Status indicates what happened to the event.
	Status Status

	// Row contains column values (only when Status == StatusOK).
	// Values are in the same order as Config.Columns.
	Row []any

	// Error contains error details (only when Status == StatusRejected or StatusError).
	Error error

	// Location indicates where the error occurred.
	// Examples: "validate[0]", "filter", "columns[2]"
	Location string
}

// IsOK returns true if the event was processed successfully.
func (r Result) IsOK() bool {
	return r.Status == StatusOK
}

// IsFiltered returns true if the event was filtered out.
func (r Result) IsFiltered() bool {
	return r.Status == StatusFiltered
}

// IsError returns true if an error occurred.
func (r Result) IsError() bool {
	return r.Status == StatusRejected || r.Status == StatusError
}

// ValidationError represents a validation failure.
type ValidationError struct {
	// Index is the index of the failed validation (0-indexed).
	Index int

	// Expression is the CEL expression that failed.
	Expression string

	// Message provides additional context (optional).
	Message string
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("validation[%d] failed: %s (expr: %s)", e.Index, e.Message, e.Expression)
	}
	return fmt.Sprintf("validation[%d] failed: %s", e.Index, e.Expression)
}

// Unwrap returns ErrValidationFailed for errors.Is compatibility.
func (e *ValidationError) Unwrap() error {
	return ErrValidationFailed
}

// ColumnError represents a column evaluation failure.
type ColumnError struct {
	// Name is the column name.
	Name string

	// Index is the column index (0-indexed).
	Index int

	// Expression is the CEL expression that failed.
	Expression string

	// Err is the underlying error.
	Err error
}

// Error implements the error interface.
func (e *ColumnError) Error() string {
	return fmt.Sprintf("column[%d] %q failed: %v (expr: %s)", e.Index, e.Name, e.Err, e.Expression)
}

// Unwrap returns the underlying error.
func (e *ColumnError) Unwrap() error {
	return e.Err
}

// FilterError represents a filter evaluation failure.
type FilterError struct {
	// Expression is the CEL expression that failed.
	Expression string

	// Err is the underlying error.
	Err error
}

func (e *FilterError) Error() string {
	return fmt.Sprintf("filter failed: %v (expr: %s)", e.Err, e.Expression)
}

func (e *FilterError) Unwrap() error {
	return e.Err
}

// CompileError represents a compilation error with location.
type CompileError struct {
	// Location indicates where the error occurred.
	// Examples: "validate[0]", "filter", "columns[2].expr"
	Location string

	// Source is the original expression text.
	Source string

	// Err is the underlying CEL error.
	Err error
}

func (e *CompileError) Error() string {
	return fmt.Sprintf("%s: %v (expr: %s)", e.Location, e.Err, e.Source)
}

func (e *CompileError) Unwrap() error {
	return e.Err
}

// CompileErrors collects multiple compilation errors.
type CompileErrors struct {
	Errors []CompileError
}

func (e *CompileErrors) Error() string {
	if len(e.Errors) == 1 {
		return e.Errors[0].Error()
	}
	return fmt.Sprintf("%d compilation errors: first: %v", len(e.Errors), e.Errors[0])
}

// Add adds a compile error to the collection.
func (e *CompileErrors) Add(err CompileError) {
	e.Errors = append(e.Errors, err)
}

// HasErrors returns true if there are any errors.
func (e *CompileErrors) HasErrors() bool {
	return len(e.Errors) > 0
}

func (e *CompileErrors) Unwrap() error {
	if len(e.Errors) == 1 {
		return e.Errors[0].Err
	}
	return nil
}

// BatchResult collects results from batch processing.
type BatchResult struct {
	// Results contains results for each event in input order.
	Results []Result

	// OKCount is the number of successfully processed events.
	OKCount int

	// FilteredCount is the number of filtered events.
	FilteredCount int

	// RejectedCount is the number of rejected events (validation failures).
	RejectedCount int

	// ErrorCount is the number of events with errors.
	ErrorCount int
}

// NewBatchResult creates a BatchResult with the given capacity.
func NewBatchResult(capacity int) *BatchResult {
	return &BatchResult{
		Results: make([]Result, 0, capacity),
	}
}

// Add adds a result to the batch.
func (b *BatchResult) Add(r Result) {
	b.Results = append(b.Results, r)
	switch r.Status {
	case StatusOK:
		b.OKCount++
	case StatusFiltered:
		b.FilteredCount++
	case StatusRejected:
		b.RejectedCount++
	case StatusError:
		b.ErrorCount++
	}
}

// Total returns the total number of results.
func (b *BatchResult) Total() int {
	return len(b.Results)
}

// HasErrors returns true if any event had an error or was rejected.
func (b *BatchResult) HasErrors() bool {
	return b.RejectedCount > 0 || b.ErrorCount > 0
}

// OKRows returns only the successful rows.
func (b *BatchResult) OKRows() [][]any {
	rows := make([][]any, 0, b.OKCount)
	for _, r := range b.Results {
		if r.Status == StatusOK {
			rows = append(rows, r.Row)
		}
	}
	return rows
}
