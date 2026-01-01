package pipeline

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/cel-go/cel"
)

// CompiledPipeline holds pre-compiled CEL programs ready for execution.
type CompiledPipeline struct {
	Config     *Config
	SchemaID   int
	SchemaName string

	// compiled expressions.
	Validations []CompiledValidation
	Filter      *CompiledFilter

	// compiled column expressions.
	Columns []CompiledColumn

	// OutputSchema is the Arrow schema for the output.
	// This is derived from the CEL types of column expressions.
	OutputSchema *arrow.Schema

	// PartitionSpec contains Iceberg-compatible partition field definitions.
	// Each field references a source column by field_id and specifies a transform.
	PartitionSpec []CompiledPartitionField

	// SortOrder contains Iceberg-compatible sort field definitions.
	// Each field references a source column by field_id with direction and null ordering.
	SortOrder []CompiledSortField
}

// CompiledValidation holds a compiled validation expression.
type CompiledValidation struct {
	// Index is the position in the validate array (for error reporting).
	Index int
	// original expression text.
	Source string
	// compiled CEL program.
	Program cel.Program
}

// CompiledFilter holds a compiled filter expression.
type CompiledFilter struct {
	Source  string
	Program cel.Program
}

// CompiledColumn holds a compiled column expression.
type CompiledColumn struct {
	// Name is the output column name.
	Name string

	// Index is the column index (0-indexed).
	Index int

	Source  string
	Program cel.Program
	// CEL output type.
	CELType *cel.Type
	// Arrow output type.
	ArrowType arrow.DataType

	// IcebergType is the Iceberg type name (from "as" field or inferred).
	IcebergType string

	// Nullable indicates whether the column can contain null values.
	Nullable bool

	// FieldID is the Iceberg-compatible field ID (optional).
	FieldID *int
}

// CompiledPartitionField holds a resolved partition field.
type CompiledPartitionField struct {
	// field_id of the source column.
	SourceID int
	FieldID  int

	// Transform specifies how to derive partition value from source.
	Transform PartitionTransform

	// Param is an optional parameter for parameterized transforms (bucket, truncate).
	Param int

	// Name is the partition key name in the output path.
	Name string

	// ColumnIndex is the resolved index into Columns array.
	ColumnIndex int
}

// CompiledSortField holds a resolved sort field.
type CompiledSortField struct {
	// field_id of the source column.
	SourceID int

	// Transform specifies how to derive sort value from source.
	Transform PartitionTransform

	// Direction is the sort order: asc or desc.
	Direction SortDirection

	// NullOrder specifies where nulls appear: nulls-first or nulls-last.
	NullOrder NullOrder

	// ColumnIndex is the resolved index into Columns array.
	ColumnIndex int
}

// ColumnNames returns all column names in order.
func (c *CompiledPipeline) ColumnNames() []string {
	names := make([]string, len(c.Columns))
	for i, col := range c.Columns {
		names[i] = col.Name
	}
	return names
}

// PartitionNames returns partition field names (for Hive-style directory paths).
func (c *CompiledPipeline) PartitionNames() []string {
	names := make([]string, len(c.PartitionSpec))
	for i, pf := range c.PartitionSpec {
		names[i] = pf.Name
	}
	return names
}

// PartitionColumnIndices returns indices into Columns for partition fields.
func (c *CompiledPipeline) PartitionColumnIndices() []int {
	indices := make([]int, len(c.PartitionSpec))
	for i, pf := range c.PartitionSpec {
		indices[i] = pf.ColumnIndex
	}
	return indices
}

// SortColumnIndices returns indices into Columns for sort fields.
func (c *CompiledPipeline) SortColumnIndices() []int {
	indices := make([]int, len(c.SortOrder))
	for i, sf := range c.SortOrder {
		indices[i] = sf.ColumnIndex
	}
	return indices
}

// HasValidation returns true if there are validation expressions.
func (c *CompiledPipeline) HasValidation() bool {
	return len(c.Validations) > 0
}

// HasFilter returns true if there is a filter expression.
func (c *CompiledPipeline) HasFilter() bool {
	return c.Filter != nil
}

// HasPartition returns true if partition spec is defined.
func (c *CompiledPipeline) HasPartition() bool {
	return len(c.PartitionSpec) > 0
}

// HasSort returns true if sort order is defined.
func (c *CompiledPipeline) HasSort() bool {
	return len(c.SortOrder) > 0
}

// ColumnCount returns the number of output columns.
func (c *CompiledPipeline) ColumnCount() int {
	return len(c.Columns)
}

// ValidationCount returns the number of validation expressions.
func (c *CompiledPipeline) ValidationCount() int {
	return len(c.Validations)
}

// GetColumn returns the column at the given index.
func (c *CompiledPipeline) GetColumn(index int) *CompiledColumn {
	if index < 0 || index >= len(c.Columns) {
		return nil
	}
	return &c.Columns[index]
}

// GetColumnByName returns the column with the given name.
func (c *CompiledPipeline) GetColumnByName(name string) *CompiledColumn {
	for i := range c.Columns {
		if c.Columns[i].Name == name {
			return &c.Columns[i]
		}
	}
	return nil
}
