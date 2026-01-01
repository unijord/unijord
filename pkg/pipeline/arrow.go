package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/cel-go/cel"
)

var (
	// ErrNilType is returned when a nil type is provided.
	ErrNilType = errors.New("nil type")

	// ErrInvalidOutputType is returned when an invalid "as" type is specified.
	ErrInvalidOutputType = errors.New("invalid output type")
)

// CEL Expression -> CEL Type -> Arrow Type -> Parquet Column
// column expression like event.amount * 100 (amount is of type int)
// CEL infers the type (int)
// Column expression
// {"name": "total", "expr": "event.amount * 100"}
// Compile expression
// ast, _ := env.Compile("event.amount * 100")
// Get CEL output type
// celType := ast.OutputType() -> cel.IntType
//  Convert to Arrow
// arrowType, _ := CELTypeToArrow(celType) -> arrow.Int64
// Build Parquet schema
// field := arrow.Field{Name: "total", Type: arrowType}
// Example Pipeline
//
//  {
//    "columns": [
//      {"name": "order_id", "expr": "event.order_id"},
//      {"name": "amount", "expr": "event.amount"},
//      {"name": "is_large", "expr": "event.amount > 100.0"},
//      {"name": "item_count", "expr": "event.items.size()"},
//      {"name": "created_at", "expr": "event.created_at"},
//      {"name": "tags", "expr": "event.tags"},
//      {"name": "metadata", "expr": "event.metadata"}
//    ]
//  }
//
//  Resulting Arrow Schema
//
//  | Column     | CEL Type         | Arrow Type          |
//  |------------|------------------|---------------------|
//  | order_id   | string           | String              |
//  | amount     | double           | Float64             |
//  | is_large   | bool             | Boolean             |
//  | item_count | int              | Int64               |
//  | created_at | timestamp        | Timestamp(µs, UTC)  |
//  | tags       | list(string)     | List                |
//  | metadata   | map(string, dyn) | Map<String, String> |

// CELTypeToArrow converts a CEL type to an Arrow data type.
//
// bool, int, uint, double, string, bytes map directly to Arrow equivalents.
// Timestamps and durations use microsecond precision.
// Lists and maps are handled recursively.
// Unknown or dynamic types serialize to JSON strings.
func CELTypeToArrow(celType *cel.Type) (arrow.DataType, error) {
	if celType == nil {
		return nil, ErrNilType
	}

	switch celType.String() {
	case "bool":
		return arrow.FixedWidthTypes.Boolean, nil
	case "int":
		return arrow.PrimitiveTypes.Int64, nil
	case "uint":
		return arrow.PrimitiveTypes.Uint64, nil
	case "double":
		return arrow.PrimitiveTypes.Float64, nil
	case "string":
		return arrow.BinaryTypes.String, nil
	case "bytes":
		return arrow.BinaryTypes.Binary, nil
	case "google.protobuf.Timestamp", "timestamp":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case "google.protobuf.Duration", "duration":
		return arrow.FixedWidthTypes.Duration_us, nil
	case "null", "null_type":
		return arrow.Null, nil
	case "dyn":
		return arrow.BinaryTypes.String, nil
	case "type":
		return arrow.BinaryTypes.String, nil
	}

	// parameterized types ?
	typeName := celType.String()

	// List types: list(T)
	if len(typeName) > 5 && typeName[:5] == "list(" {
		params := celType.Parameters()
		if len(params) > 0 {
			elemType, err := CELTypeToArrow(params[0])
			if err != nil {
				return nil, fmt.Errorf("list element type: %w", err)
			}
			return arrow.ListOf(elemType), nil
		}
		// Fallback for list without parameters
		return arrow.ListOf(arrow.BinaryTypes.String), nil
	}

	// Map types: map(K, V)
	if len(typeName) > 4 && typeName[:4] == "map(" {
		params := celType.Parameters()
		if len(params) >= 2 {
			keyType, err := CELTypeToArrow(params[0])
			if err != nil {
				return nil, fmt.Errorf("map key type: %w", err)
			}
			valType, err := CELTypeToArrow(params[1])
			if err != nil {
				return nil, fmt.Errorf("map value type: %w", err)
			}
			return arrow.MapOf(keyType, valType), nil
		}
		// Fallback for map without parameters
		return arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), nil
	}

	// Handle custom object types - serialize as JSON string
	return arrow.BinaryTypes.String, nil
}

// ParseOutputType parses an "as" type string into an Arrow data type.
// Supported types match common analytics formats (Parquet, Iceberg, etc.):
//   - boolean, int, long, float, double: primitive numerics
//   - string, binary: byte sequences
//   - date, time: calendar/clock types
//   - timestamp, timestamptz: datetime with/without timezone
//   - decimal(P,S): fixed-point decimal with precision P and scale S
//   - uuid: 16-byte UUID
//
// Source:
// https://github.com/apache/iceberg/blob/main/format/spec.md
func ParseOutputType(typeStr string) (arrow.DataType, error) {
	if typeStr == "" {
		return nil, nil
	}
	typeStr = strings.ToLower(typeStr)

	switch typeStr {
	case "boolean", "bool":
		return arrow.FixedWidthTypes.Boolean, nil
	case "int", "int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "long", "int64":
		return arrow.PrimitiveTypes.Int64, nil
	case "float", "float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "double", "float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "string":
		return arrow.BinaryTypes.String, nil
	case "binary", "bytes":
		return arrow.BinaryTypes.Binary, nil
	case "date":
		return arrow.FixedWidthTypes.Date32, nil
	case "time":
		return arrow.FixedWidthTypes.Time64us, nil
	case "timestamp":
		// local datetime
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case "timestamptz":
		// UTC
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case "uuid":
		return &arrow.FixedSizeBinaryType{ByteWidth: 16}, nil
	}

	// check for decimal(P,S)
	if len(typeStr) > 8 && typeStr[:8] == "decimal(" && typeStr[len(typeStr)-1] == ')' {
		params := typeStr[8 : len(typeStr)-1]
		var precision, scale int32
		n, err := fmt.Sscanf(params, "%d,%d", &precision, &scale)
		if err != nil || n != 2 {
			return nil, fmt.Errorf("%w: %s (expected decimal(P,S))", ErrInvalidOutputType, typeStr)
		}
		if precision <= 0 || precision > 38 {
			return nil, fmt.Errorf("%w: %s (precision must be 1-38)", ErrInvalidOutputType, typeStr)
		}
		if scale < 0 || scale > precision {
			return nil, fmt.Errorf("%w: %s (scale must be 0 to precision)", ErrInvalidOutputType, typeStr)
		}
		return &arrow.Decimal128Type{Precision: precision, Scale: scale}, nil
	}

	return nil, fmt.Errorf("%w: %s", ErrInvalidOutputType, typeStr)
}

// BuildArrowSchema creates an Arrow schema from compiled columns.
func BuildArrowSchema(columns []CompiledColumn) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(columns))

	for i, col := range columns {
		arrowType := col.ArrowType
		if arrowType == nil {
			var err error
			arrowType, err = CELTypeToArrow(col.CELType)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col.Name, err)
			}
		}

		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: col.Nullable,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// Arrow data to Parquet, metadata is preserved in the file footer.
// Hive-style directory partitioning is supported via partition_spec.

// BuildArrowSchemaWithMetadata creates Arrow schema with Iceberg-compatible metadata.
// This includes:
//   - Field-level metadata: PARQUET:field_id for each column
//   - Schema-level metadata: iceberg.schema JSON for full schema information
//
// The iceberg.schema metadata follows the Apache Iceberg specification format,
// enabling schema evolution and compatibility with Iceberg readers.
//
// The schemaID parameter is used in the iceberg.schema metadata to track schema versions.
// This should match the schema ID used during registration (e.g., RegisterAVRO(100, ...)).
func BuildArrowSchemaWithMetadata(
	columns []CompiledColumn,
	partitionSpec []CompiledPartitionField,
	sortOrder []CompiledSortField,
	schemaID int,
) (*arrow.Schema, error) {
	// check if column is used for partition and sorting.
	// get the column index map.
	partitionIndices := make(map[int]bool)
	for _, pf := range partitionSpec {
		partitionIndices[pf.ColumnIndex] = true
	}
	sortIndices := make(map[int]bool)
	for _, sf := range sortOrder {
		sortIndices[sf.ColumnIndex] = true
	}

	fields := make([]arrow.Field, len(columns))

	for i, col := range columns {
		arrowType := col.ArrowType
		if arrowType == nil {
			var err error
			arrowType, err = CELTypeToArrow(col.CELType)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col.Name, err)
			}
		}

		var metaKeys, metaVals []string

		// Iceberg field ID if specified
		if col.FieldID != nil {
			metaKeys = append(metaKeys, "PARQUET:field_id")
			metaVals = append(metaVals, strconv.Itoa(*col.FieldID))
		}

		// partition marker if column is used in partition spec
		// hint
		if partitionIndices[i] {
			metaKeys = append(metaKeys, "partition")
			metaVals = append(metaVals, "true")
		}

		// sort marker if column is used in sort order
		if sortIndices[i] {
			metaKeys = append(metaKeys, "sort")
			metaVals = append(metaVals, "true")
		}

		var meta arrow.Metadata
		if len(metaKeys) > 0 {
			meta = arrow.NewMetadata(metaKeys, metaVals)
		}

		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: col.Nullable,
			Metadata: meta,
		}
	}

	// Build schema-level metadata with iceberg.schema
	schemaMeta, err := buildIcebergSchemaMetadata(columns, schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to build iceberg schema metadata: %w", err)
	}

	return arrow.NewSchema(fields, schemaMeta), nil
}

// IcebergSchema represents the Iceberg schema JSON format.
// Reference: https://iceberg.apache.org/spec/#schemas
type IcebergSchema struct {
	Type     string         `json:"type"`
	SchemaID int            `json:"schema-id"`
	Fields   []IcebergField `json:"fields"`
}

// IcebergField represents a field in the Iceberg schema.
type IcebergField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
}

// buildIcebergSchemaMetadata creates schema-level metadata containing iceberg.schema.
// The schemaID is used in the iceberg.schema JSON to identify the schema version.
func buildIcebergSchemaMetadata(columns []CompiledColumn, schemaID int) (*arrow.Metadata, error) {
	icebergFields := make([]IcebergField, 0, len(columns))

	for _, col := range columns {
		// include columns with field IDs in iceberg.schema
		if col.FieldID == nil {
			continue
		}

		// IcebergType from column mostly set from "as" field or inferred from Arrow type.
		icebergType := col.IcebergType
		if icebergType == "" {
			icebergType = arrowTypeToIcebergType(col.ArrowType)
		}

		icebergFields = append(icebergFields, IcebergField{
			ID:       *col.FieldID,
			Name:     col.Name,
			Required: !col.Nullable,
			Type:     icebergType,
		})
	}

	// no schema metadata.
	if len(icebergFields) == 0 {
		return nil, nil
	}

	schema := IcebergSchema{
		Type:     "struct",
		SchemaID: schemaID,
		Fields:   icebergFields,
	}

	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("marshal iceberg schema: %w", err)
	}

	meta := arrow.NewMetadata(
		[]string{"iceberg.schema"},
		[]string{string(schemaJSON)},
	)
	return &meta, nil
}

// arrowTypeToIcebergType converts Arrow data type to Iceberg type string.
// Reference: https://iceberg.apache.org/spec/#primitive-types
func arrowTypeToIcebergType(arrowType arrow.DataType) string {
	if arrowType == nil {
		return "string"
	}

	switch arrowType.ID() {
	case arrow.BOOL:
		return "boolean"
	case arrow.INT8, arrow.INT16, arrow.INT32:
		return "int"
	case arrow.INT64:
		return "long"
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return "long"
	case arrow.FLOAT32:
		return "float"
	case arrow.FLOAT64:
		return "double"
	case arrow.STRING, arrow.LARGE_STRING:
		return "string"
	case arrow.BINARY, arrow.LARGE_BINARY:
		return "binary"
	case arrow.DATE32, arrow.DATE64:
		return "date"
	case arrow.TIME32, arrow.TIME64:
		return "time"
	case arrow.TIMESTAMP:
		ts := arrowType.(*arrow.TimestampType)
		if ts.TimeZone != "" {
			return "timestamptz"
		}
		return "timestamp"
	case arrow.DURATION:
		// Iceberg doesn't have duration, use long
		return "long"
	case arrow.DECIMAL128, arrow.DECIMAL256:
		dec := arrowType.(arrow.DecimalType)
		return fmt.Sprintf("decimal(%d,%d)", dec.GetPrecision(), dec.GetScale())
	case arrow.FIXED_SIZE_BINARY:
		fsb := arrowType.(*arrow.FixedSizeBinaryType)
		if fsb.ByteWidth == 16 {
			return "uuid"
		}
		return fmt.Sprintf("fixed[%d]", fsb.ByteWidth)
	case arrow.LIST, arrow.LARGE_LIST:
		listType := arrowType.(arrow.ListLikeType)
		elemType := arrowTypeToIcebergType(listType.Elem())
		return fmt.Sprintf("list<%s>", elemType)
	case arrow.MAP:
		mapType := arrowType.(*arrow.MapType)
		keyType := arrowTypeToIcebergType(mapType.KeyType())
		valType := arrowTypeToIcebergType(mapType.ItemType())
		return fmt.Sprintf("map<%s,%s>", keyType, valType)
	default:
		return "string"
	}
}

// IsNullableCELType returns true if the CEL type can represent null.
func IsNullableCELType(celType *cel.Type) bool {
	if celType == nil {
		return true
	}
	switch celType.String() {
	case "null", "null_type", "dyn":
		return true
	default:
		return false
	}
}

// ArrowFieldFromColumn creates an Arrow field from a compiled column.
func ArrowFieldFromColumn(col CompiledColumn) (arrow.Field, error) {
	arrowType := col.ArrowType
	if arrowType == nil {
		var err error
		arrowType, err = CELTypeToArrow(col.CELType)
		if err != nil {
			return arrow.Field{}, fmt.Errorf("column %q: %w", col.Name, err)
		}
	}

	return arrow.Field{
		Name:     col.Name,
		Type:     arrowType,
		Nullable: col.Nullable,
	}, nil
}
