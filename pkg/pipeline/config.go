package pipeline

import (
	"errors"
	"fmt"
	"strings"
)

// SchemaFormat indicates the schema type.
type SchemaFormat string

const (
	// SchemaFormatJSON indicates JSON Schema format.
	SchemaFormatJSON SchemaFormat = "json"

	// SchemaFormatAvro indicates Avro schema format.
	SchemaFormatAvro SchemaFormat = "avro"
)

// SortDirection defines the sort order direction.
type SortDirection string

const (
	SortAsc  SortDirection = "asc"
	SortDesc SortDirection = "desc"
)

// NullOrder defines where nulls appear in sorted output.
//
//	Data: [3, NULL, 1, NULL, 2]
//
//	nulls-first + asc: [NULL, NULL, 1, 2, 3]
//	nulls-last  + asc: [1, 2, 3, NULL, NULL]
//
//	nulls-first + desc: [NULL, NULL, 3, 2, 1]
//	nulls-last  + desc: [3, 2, 1, NULL, NULL]
type NullOrder string

const (
	NullsFirst NullOrder = "nulls-first"
	NullsLast  NullOrder = "nulls-last"
)

var (
	// ErrNoColumns is returned when Columns is empty.
	ErrNoColumns = errors.New("pipeline must have at least one column")
	// ErrInvalidSchemaFormat is returned when schema_format is invalid.
	ErrInvalidSchemaFormat = errors.New("invalid schema_format: must be 'json' or 'avro'")
	// ErrEmptyColumnName is returned when a column has an empty name.
	ErrEmptyColumnName = errors.New("column name cannot be empty")
	// ErrEmptyColumnExpr is returned when a column has an empty expression.
	ErrEmptyColumnExpr = errors.New("column expression cannot be empty")
	// ErrDuplicateColumnName is returned when two columns have the same name.
	ErrDuplicateColumnName = errors.New("duplicate column name")
	// ErrEmptyValidateExpr is returned when a validate expression is empty.
	ErrEmptyValidateExpr = errors.New("validate expression cannot be empty")
	// ErrDuplicateFieldID is returned when two columns have the same field_id.
	ErrDuplicateFieldID = errors.New("duplicate field_id")
	// ErrInvalidFieldID is returned when field_id is out of valid range.
	ErrInvalidFieldID = errors.New("field_id must be positive and less than 2147483447")
	// ErrInvalidPartitionSpec is returned when partition_spec is invalid.
	ErrInvalidPartitionSpec = errors.New("invalid partition_spec")
	// ErrUnknownSourceID is returned when source_id references unknown column.
	ErrUnknownSourceID = errors.New("source_id references unknown column field_id")
	// ErrDuplicatePartitionFieldID is returned when partition field_id is not unique.
	ErrDuplicatePartitionFieldID = errors.New("duplicate partition field_id")
	// ErrInvalidTransform is returned when transform is not recognized.
	ErrInvalidTransform = errors.New("invalid transform")
	// ErrInvalidSortDirection is returned when sort direction is invalid.
	ErrInvalidSortDirection = errors.New("invalid sort direction: must be 'asc' or 'desc'")
	// ErrInvalidNullOrder is returned when null order is invalid.
	ErrInvalidNullOrder = errors.New("invalid null_order: must be 'nulls-first' or 'nulls-last'")
)

// Config represents the x-pipeline configuration embedded in a schema.
// It defines how events should be validated, filtered, and transformed.
type Config struct {
	// SchemaID is the unique identifier for this schema.
	SchemaID int `json:"schema_id,omitempty"`

	// SchemaFormat specifies the schema type: "json" or "avro".
	SchemaFormat SchemaFormat `json:"schema_format,omitempty"`

	// Validations contains boolean expressions that must all return true.
	// Events failing any validation are rejected with an error.
	Validations []string `json:"validate,omitempty"`

	// Filter is a boolean expression. Events where filter returns false
	// are silently dropped (not an error).
	Filter string `json:"filter,omitempty"`

	// Columns defines the output schema. Each column is a CEL expression
	// evaluated against the input event.
	Columns []Column `json:"columns"`

	// PartitionSpec defines how to partition output data.
	// Each field specifies a source column and transform.
	PartitionSpec []PartitionField `json:"partition_spec,omitempty"`

	// SortOrder defines how to sort data within partitions.
	// Multiple fields create a composite sort key.
	SortOrder []SortField `json:"sort_order,omitempty"`
}

// PartitionField defines a single partition dimension.
type PartitionField struct {
	// SourceID is the field_id of the source column from Columns.
	SourceID int `json:"source_id"`

	// FieldID is a unique identifier for this partition field.
	// Convention: use 1000+ as in ICEBERG to separate from column field_ids.
	FieldID int `json:"field_id"`

	// Transform specifies how to derive partition value from source.
	// Options: identity, bucket, truncate, year, month, day, hour, void
	Transform PartitionTransform `json:"transform"`

	// Param is an optional parameter for parameterized transforms.
	// Used by bucket[N] and truncate[W].
	Param int `json:"param,omitempty"`

	// Name is the partition key name in the output path.
	// Example: "dt" produces directories like "dt=2024-12-31"
	Name string `json:"name"`
}

// SortField defines a single sort dimension.
type SortField struct {
	// SourceID is the field_id of the source column from Columns.
	SourceID int `json:"source_id"`

	// Transform specifies how to derive sort value from source.
	// Usually "identity" for sorting by the raw value.
	Transform PartitionTransform `json:"transform"`

	// Direction is the sort order: "asc" or "desc".
	Direction SortDirection `json:"direction"`

	// NullOrder specifies where nulls appear: "nulls-first" or "nulls-last".
	NullOrder NullOrder `json:"null_order"`
}

// Column defines an output column.
type Column struct {
	// Name is the output column name. Must be unique within Columns.
	Name string `json:"name"`

	// Expr is a CEL expression evaluated against the input event.
	// The expression's return type determines the column's data type.
	Expr string `json:"expr"`

	// FieldID is an optional field ID for schema evolution support.
	// When specified, this ID is written to Parquet file metadata
	// for proper column mapping during schema evolution.
	// Must be unique within the table and less than 2147483447- ICEBERG Limit.
	// https://iceberg.apache.org/spec/#reserved-field-ids MAX-200
	FieldID *int `json:"field_id,omitempty"`

	// As overrides the CEL-inferred type with an explicit output type.
	// Useful when the target system requires a specific type that
	// CEL doesn't natively support.
	// Supported values: date, time, timestamp, timestamptz, float, int,
	// decimal(P,S), uuid, binary, string, boolean, long, double.
	As string `json:"as,omitempty"`
}

// Validate checks if the configuration is structurally valid.
func (c *Config) Validate() error {
	if c.SchemaFormat != "" {
		format := SchemaFormat(strings.ToLower(string(c.SchemaFormat)))
		if format != SchemaFormatJSON && format != SchemaFormatAvro {
			return ErrInvalidSchemaFormat
		}
	}

	if len(c.Columns) == 0 {
		return ErrNoColumns
	}

	seen := make(map[string]bool, len(c.Columns))
	fieldIDs := make(map[int]bool)
	for i, col := range c.Columns {
		if col.Name == "" {
			return fmt.Errorf("column[%d]: %w", i, ErrEmptyColumnName)
		}
		if col.Expr == "" {
			return fmt.Errorf("column[%d] %q: %w", i, col.Name, ErrEmptyColumnExpr)
		}
		if seen[col.Name] {
			return fmt.Errorf("column[%d]: %w: %q", i, ErrDuplicateColumnName, col.Name)
		}
		seen[col.Name] = true

		if col.FieldID != nil {
			id := *col.FieldID
			if id <= 0 || id >= 2147483447 {
				return fmt.Errorf("column[%d] %q: %w", i, col.Name, ErrInvalidFieldID)
			}
			if fieldIDs[id] {
				return fmt.Errorf("column[%d] %q: %w: %d", i, col.Name, ErrDuplicateFieldID, id)
			}
			fieldIDs[id] = true
		}
	}

	for i, expr := range c.Validations {
		if expr == "" {
			return fmt.Errorf("validate[%d]: %w", i, ErrEmptyValidateExpr)
		}
	}

	if err := c.validatePartitionSpec(fieldIDs); err != nil {
		return err
	}

	if err := c.validateSortOrder(fieldIDs); err != nil {
		return err
	}

	return nil
}

// validatePartitionSpec validates the partition specification.
func (c *Config) validatePartitionSpec(columnFieldIDs map[int]bool) error {
	partitionFieldIDs := make(map[int]bool)

	for i, pf := range c.PartitionSpec {
		if !columnFieldIDs[pf.SourceID] {
			return fmt.Errorf("partition_spec[%d]: %w: source_id %d not found (hint: ensure target column has field_id: %d)", i, ErrUnknownSourceID, pf.SourceID, pf.SourceID)
		}

		if pf.FieldID <= 0 {
			return fmt.Errorf("partition_spec[%d]: %w: field_id must be positive", i, ErrInvalidPartitionSpec)
		}
		if partitionFieldIDs[pf.FieldID] {
			return fmt.Errorf("partition_spec[%d]: %w: %d", i, ErrDuplicatePartitionFieldID, pf.FieldID)
		}
		partitionFieldIDs[pf.FieldID] = true

		if !isValidTransform(pf.Transform) {
			return fmt.Errorf("partition_spec[%d]: %w: %q", i, ErrInvalidTransform, pf.Transform)
		}

		if pf.Name == "" {
			return fmt.Errorf("partition_spec[%d]: %w: name cannot be empty", i, ErrInvalidPartitionSpec)
		}
	}

	return nil
}

// validateSortOrder validates the sort order specification.
func (c *Config) validateSortOrder(columnFieldIDs map[int]bool) error {
	for i, sf := range c.SortOrder {
		if !columnFieldIDs[sf.SourceID] {
			return fmt.Errorf("sort_order[%d]: %w: source_id %d not found (hint: ensure target column has field_id: %d)", i, ErrUnknownSourceID, sf.SourceID, sf.SourceID)
		}

		if !isValidTransform(sf.Transform) {
			return fmt.Errorf("sort_order[%d]: %w: %q", i, ErrInvalidTransform, sf.Transform)
		}

		if sf.Direction != SortAsc && sf.Direction != SortDesc {
			return fmt.Errorf("sort_order[%d]: %w: %q", i, ErrInvalidSortDirection, sf.Direction)
		}

		if sf.NullOrder != NullsFirst && sf.NullOrder != NullsLast {
			return fmt.Errorf("sort_order[%d]: %w: %q", i, ErrInvalidNullOrder, sf.NullOrder)
		}
	}

	return nil
}

// isValidTransform checks if the transform is recognized.
func isValidTransform(t PartitionTransform) bool {
	switch t {
	case TransformIdentity, TransformBucket, TransformTruncate,
		TransformYear, TransformMonth, TransformDay, TransformHour, TransformVoid:
		return true
	default:
		return false
	}
}

// ColumnNames returns all column names in order.
func (c *Config) ColumnNames() []string {
	names := make([]string, len(c.Columns))
	for i, col := range c.Columns {
		names[i] = col.Name
	}
	return names
}

// ColumnByFieldID returns the column with the given field_id, or nil if not found.
func (c *Config) ColumnByFieldID(fieldID int) *Column {
	for i := range c.Columns {
		if c.Columns[i].FieldID != nil && *c.Columns[i].FieldID == fieldID {
			return &c.Columns[i]
		}
	}
	return nil
}

// ColumnIndex returns the index of a column by name, or -1 if not found.
func (c *Config) ColumnIndex(name string) int {
	for i, col := range c.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// HasValidation returns true if there are validation expressions.
func (c *Config) HasValidation() bool {
	return len(c.Validations) > 0
}

// HasFilter returns true if there is a filter expression.
func (c *Config) HasFilter() bool {
	return c.Filter != ""
}

// HasPartitionSpec returns true if partition spec is defined.
func (c *Config) HasPartitionSpec() bool {
	return len(c.PartitionSpec) > 0
}

// HasSortOrder returns true if sort order is defined.
func (c *Config) HasSortOrder() bool {
	return len(c.SortOrder) > 0
}

// GetSchemaFormat returns the normalized schema format.
func (c *Config) GetSchemaFormat() SchemaFormat {
	if c.SchemaFormat == "" {
		return ""
	}
	return SchemaFormat(strings.ToLower(string(c.SchemaFormat)))
}

// IsJSON returns true if schema format is JSON Schema.
func (c *Config) IsJSON() bool {
	return c.GetSchemaFormat() == SchemaFormatJSON
}

// IsAvro returns true if schema format is Avro.
func (c *Config) IsAvro() bool {
	return c.GetSchemaFormat() == SchemaFormatAvro
}
