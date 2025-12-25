package schema

import (
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/hamba/avro/v2"
)

// AvroMapper parses Avro schemas for use with the TypeProvider.
type AvroMapper struct{}

// NewAvroMapper creates a new AvroMapper.
func NewAvroMapper() *AvroMapper {
	return &AvroMapper{}
}

// Map parses Avro schema JSON and returns a MappedSchema.
func (m *AvroMapper) Map(schema []byte) (*MappedSchema, error) {
	parsed, err := avro.Parse(string(schema))
	if err != nil {
		return nil, err
	}
	return m.FromSchema(parsed)
}

// FromSchema creates a MappedSchema from an already parsed avro.Schema.
func (m *AvroMapper) FromSchema(schema avro.Schema) (*MappedSchema, error) {
	if _, ok := schema.(*avro.RecordSchema); !ok {
		return nil, fmt.Errorf("expected record schema, got %s", schema.Type())
	}
	return &MappedSchema{Raw: schema}, nil
}

// AvroAdapter implements SchemaAdapter for Avro schemas.
type AvroAdapter struct {
	schema *avro.RecordSchema
}

// NewAvroAdapter creates an adapter from a MappedSchema containing an Avro schema.
func NewAvroAdapter(mapped *MappedSchema) (*AvroAdapter, error) {
	if mapped == nil {
		return nil, ErrNilSchema
	}

	raw, ok := mapped.Raw.(avro.Schema)
	if !ok {
		return nil, fmt.Errorf("%w: got %T", ErrNotAvroSchema, mapped.Raw)
	}

	rec, ok := raw.(*avro.RecordSchema)
	if !ok {
		return nil, fmt.Errorf("%w: got %s", ErrNotRecordSchema, raw.Type())
	}

	return &AvroAdapter{schema: rec}, nil
}

// NewAvroAdapterFromSchema creates an adapter directly from a parsed avro.RecordSchema.
func NewAvroAdapterFromSchema(schema *avro.RecordSchema) *AvroAdapter {
	return &AvroAdapter{schema: schema}
}

// BuildTypes implements SchemaAdapter.
func (a *AvroAdapter) BuildTypes(provider *TypeProvider) (string, error) {
	if a.schema == nil {
		return "", ErrNilSchema
	}
	rootType := a.buildProvider(a.schema, provider)
	return rootType, nil
}

func (a *AvroAdapter) buildProvider(schema avro.Schema, p *TypeProvider) string {
	rec, ok := schema.(*avro.RecordSchema)
	if !ok {
		return ""
	}

	name := rec.FullName()
	fields := make(map[string]*cel.Type)

	for _, f := range rec.Fields() {
		fields[f.Name()] = a.mapTypeToCel(f.Type(), p)
	}

	p.registerType(name, fields)
	return name
}

func (a *AvroAdapter) mapTypeToCel(schema avro.Schema, p *TypeProvider) *cel.Type {
	switch s := schema.(type) {
	case *avro.PrimitiveSchema:
		return a.mapPrimitiveToCel(s)

	case *avro.RecordSchema:
		name := a.buildProvider(s, p)
		return cel.ObjectType(name)

	case *avro.ArraySchema:
		elemType := a.mapTypeToCel(s.Items(), p)
		return cel.ListType(elemType)

	case *avro.MapSchema:
		valType := a.mapTypeToCel(s.Values(), p)
		return cel.MapType(cel.StringType, valType)

	case *avro.UnionSchema:
		return a.mapUnionToCel(s, p)

	case *avro.EnumSchema:
		return cel.StringType

	case *avro.FixedSchema:
		return cel.BytesType

	case *avro.RefSchema:
		return a.mapTypeToCel(s.Schema(), p)

	default:
		return cel.DynType
	}
}

func (a *AvroAdapter) mapPrimitiveToCel(s *avro.PrimitiveSchema) *cel.Type {
	if s.Logical() != nil {
		switch s.Logical().Type() {
		case "timestamp-millis", "timestamp-micros",
			"local-timestamp-millis", "local-timestamp-micros":
			return cel.TimestampType
		case "date":
			return cel.TimestampType
		case "time-millis", "time-micros":
			return cel.DurationType
		case "uuid":
			return cel.StringType
		case "decimal":
			return cel.StringType
		}
	}

	switch s.Type() {
	case "null":
		return cel.NullType
	case "boolean":
		return cel.BoolType
	case "int", "long":
		return cel.IntType
	case "float", "double":
		return cel.DoubleType
	case "string":
		return cel.StringType
	case "bytes":
		return cel.BytesType
	default:
		return cel.DynType
	}
}

// Avro uses unions for nullable/optional fields:
//
//	{"name": "nickname", "type": ["null", "string"]}
//	{"name": "value", "type": ["null", "int", "string"]}-Multiple non-null types
func (a *AvroAdapter) mapUnionToCel(s *avro.UnionSchema, p *TypeProvider) *cel.Type {
	types := s.Types()
	if len(types) == 0 {
		return cel.DynType
	}

	var nonNullType *cel.Type
	for _, t := range types {
		if t.Type() == "null" {
			continue
		}
		if nonNullType == nil {
			nonNullType = a.mapTypeToCel(t, p)
		} else {
			return cel.DynType
		}
	}

	if nonNullType == nil {
		return cel.NullType
	}
	return nonNullType
}
