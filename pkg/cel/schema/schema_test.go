package schema

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/hamba/avro/v2"
)

func TestAvroMapper_ValidSchema(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Test",
		"namespace": "com.example",
		"fields": [
			{"name": "flag", "type": "boolean"},
			{"name": "count", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	m := NewAvroMapper()
	mapped, err := m.Map([]byte(schema))
	if err != nil {
		t.Fatalf("Map error: %v", err)
	}

	if mapped.Raw == nil {
		t.Fatal("Raw should not be nil")
	}

	rec, ok := mapped.Raw.(*avro.RecordSchema)
	if !ok {
		t.Fatal("Raw should be *avro.RecordSchema")
	}

	if rec.FullName() != "com.example.Test" {
		t.Errorf("FullName = %s, want com.example.Test", rec.FullName())
	}

	if len(rec.Fields()) != 3 {
		t.Errorf("Fields count = %d, want 3", len(rec.Fields()))
	}
}

func TestAvroMapper_InvalidSchema(t *testing.T) {
	m := NewAvroMapper()

	// Invalid JSON
	_, err := m.Map([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}

	// Non-record schema
	_, err = m.Map([]byte(`{"type": "string"}`))
	if err == nil {
		t.Error("expected error for non-record schema")
	}
}

func TestNewAvroAdapter_Errors(t *testing.T) {
	// Nil schema
	_, err := NewAvroAdapter(nil)
	if err != ErrNilSchema {
		t.Errorf("expected ErrNilSchema, got %v", err)
	}

	// Non-Avro schema (wrong type in Raw)
	badSchema := &MappedSchema{Raw: "not an avro schema"}
	_, err = NewAvroAdapter(badSchema)
	if err == nil {
		t.Error("expected error for non-Avro schema")
	}

	// Non-record Avro schema
	arraySchema, _ := avro.Parse(`{"type": "array", "items": "string"}`)
	nonRecordSchema := &MappedSchema{Raw: arraySchema}
	_, err = NewAvroAdapter(nonRecordSchema)
	if err == nil {
		t.Error("expected error for non-record schema")
	}
}

func TestAvroMapper_ComplexSchema(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Order",
		"fields": [
			{"name": "order_id", "type": "string"},
			{"name": "customer", "type": {
				"type": "record",
				"name": "Customer",
				"fields": [
					{"name": "name", "type": "string"},
					{"name": "age", "type": "int"}
				]
			}},
			{"name": "items", "type": {"type": "array", "items": {
				"type": "record",
				"name": "Item",
				"fields": [
					{"name": "sku", "type": "string"},
					{"name": "price", "type": "double"}
				]
			}}},
			{"name": "metadata", "type": {"type": "map", "values": "string"}}
		]
	}`

	m := NewAvroMapper()
	mapped, err := m.Map([]byte(schema))
	if err != nil {
		t.Fatalf("Map error: %v", err)
	}

	rec := mapped.Raw.(*avro.RecordSchema)
	if len(rec.Fields()) != 4 {
		t.Errorf("Fields count = %d, want 4", len(rec.Fields()))
	}
}

func TestAvroMapper_LogicalTypes(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
			{"name": "event_date", "type": {"type": "int", "logicalType": "date"}},
			{"name": "event_id", "type": {"type": "string", "logicalType": "uuid"}}
		]
	}`

	m := NewAvroMapper()
	mapped, err := m.Map([]byte(schema))
	if err != nil {
		t.Fatalf("Map error: %v", err)
	}

	rec := mapped.Raw.(*avro.RecordSchema)
	if len(rec.Fields()) != 3 {
		t.Errorf("Fields count = %d, want 3", len(rec.Fields()))
	}
}

func TestAvroMapper_Unions(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Test",
		"fields": [
			{"name": "optional_name", "type": ["null", "string"]},
			{"name": "optional_record", "type": ["null", {
				"type": "record",
				"name": "Nested",
				"fields": [{"name": "id", "type": "int"}]
			}]}
		]
	}`

	m := NewAvroMapper()
	mapped, err := m.Map([]byte(schema))
	if err != nil {
		t.Fatalf("Map error: %v", err)
	}

	rec := mapped.Raw.(*avro.RecordSchema)
	if len(rec.Fields()) != 2 {
		t.Errorf("Fields count = %d, want 2", len(rec.Fields()))
	}
}

// Integration test: AvroMapper + TypeProvider + BuildTypedEnv
func TestAvroMapper_WithTypeProvider(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Order",
		"namespace": "com.example",
		"fields": [
			{"name": "amount", "type": "double"},
			{"name": "customer", "type": {
				"type": "record",
				"name": "Customer",
				"fields": [
					{"name": "name", "type": "string"},
					{"name": "age", "type": "int"}
				]
			}}
		]
	}`

	m := NewAvroMapper()
	mapped, err := m.Map([]byte(schema))
	if err != nil {
		t.Fatalf("Map error: %v", err)
	}

	adapter, err := NewAvroAdapter(mapped)
	if err != nil {
		t.Fatalf("NewAvroAdapter error: %v", err)
	}

	provider := NewTypeProvider()
	env, rootType, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	if rootType != "com.example.Order" {
		t.Errorf("rootType = %s, want com.example.Order", rootType)
	}

	// Valid expression - should compile
	_, issues := env.Compile("event.customer.age > 18")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected valid expression to compile: %v", issues.Err())
	}

	// Invalid expression - should fail compile
	_, issues = env.Compile("event.customer.ageeee > 18")
	if issues == nil || issues.Err() == nil {
		t.Error("expected compile error for typo")
	}
}

func TestBuildTypedEnv_CustomEnvelope(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "value", "type": "int"}
		]
	}`

	m := NewAvroMapper()
	mapped, err := m.Map([]byte(schema))
	if err != nil {
		t.Fatalf("Map error: %v", err)
	}

	adapter, err := NewAvroAdapter(mapped)
	if err != nil {
		t.Fatalf("NewAvroAdapter error: %v", err)
	}

	provider := NewTypeProvider()
	opts := EnvOptions{
		EnvelopeFields: []EnvelopeField{
			{Name: "_custom_field", Type: cel.StringType},
			{Name: "_tenant_id", Type: cel.IntType},
		},
	}

	env, _, err := BuildTypedEnv(adapter, provider, opts)
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	// Should have access to custom fields
	_, issues := env.Compile("_custom_field == 'test'")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected _custom_field to be available: %v", issues.Err())
	}

	_, issues = env.Compile("_tenant_id > 0")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected _tenant_id to be available: %v", issues.Err())
	}

	// Should also have default envelope fields
	_, issues = env.Compile("_event_type == 'test'")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected default envelope fields to be available: %v", issues.Err())
	}
}

func TestBuildTypedEnv_DisableDefaultEnvelope(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "value", "type": "int"}
		]
	}`

	m := NewAvroMapper()
	mapped, err := m.Map([]byte(schema))
	if err != nil {
		t.Fatalf("Map error: %v", err)
	}

	adapter, err := NewAvroAdapter(mapped)
	if err != nil {
		t.Fatalf("NewAvroAdapter error: %v", err)
	}

	provider := NewTypeProvider()
	opts := EnvOptions{
		DisableDefaultEnvelope: true,
		EnvelopeFields: []EnvelopeField{
			{Name: "_only_field", Type: cel.StringType},
		},
	}

	env, _, err := BuildTypedEnv(adapter, provider, opts)
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	// Custom field should work
	_, issues := env.Compile("_only_field == 'test'")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected _only_field to be available: %v", issues.Err())
	}

	// Default envelope fields should NOT be available
	_, issues = env.Compile("_event_type == 'test'")
	if issues == nil || issues.Err() == nil {
		t.Error("expected _event_type to NOT be available when default envelope is disabled")
	}
}

func TestBuildTypedEnv_CustomEventVarName(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Message",
		"fields": [
			{"name": "content", "type": "string"}
		]
	}`

	m := NewAvroMapper()
	mapped, err := m.Map([]byte(schema))
	if err != nil {
		t.Fatalf("Map error: %v", err)
	}

	adapter, err := NewAvroAdapter(mapped)
	if err != nil {
		t.Fatalf("NewAvroAdapter error: %v", err)
	}

	provider := NewTypeProvider()
	opts := EnvOptions{
		EventVarName:           "msg",
		DisableDefaultEnvelope: true,
	}

	env, _, err := BuildTypedEnv(adapter, provider, opts)
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	// Custom var name should work
	_, issues := env.Compile("msg.content == 'hello'")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected msg.content to work: %v", issues.Err())
	}

	// Default "event" should NOT work
	_, issues = env.Compile("event.content == 'hello'")
	if issues == nil || issues.Err() == nil {
		t.Error("expected 'event' to NOT be available when custom var name is used")
	}
}

func TestNewAvroAdapterFromSchema(t *testing.T) {
	schemaJSON := `{
		"type": "record",
		"name": "Direct",
		"fields": [
			{"name": "id", "type": "int"}
		]
	}`

	parsed, err := avro.Parse(schemaJSON)
	if err != nil {
		t.Fatalf("avro.Parse error: %v", err)
	}

	rec := parsed.(*avro.RecordSchema)
	adapter := NewAvroAdapterFromSchema(rec)

	provider := NewTypeProvider()
	rootType, err := adapter.BuildTypes(provider)
	if err != nil {
		t.Fatalf("BuildTypes error: %v", err)
	}

	if rootType != "Direct" {
		t.Errorf("rootType = %s, want Direct", rootType)
	}
}

func TestBuildTypedEnv_NilAdapter(t *testing.T) {
	provider := NewTypeProvider()
	opts := DefaultEnvOptions()

	_, _, err := BuildTypedEnv(nil, provider, opts)
	if err != ErrNilAdapter {
		t.Errorf("expected ErrNilAdapter, got %v", err)
	}
}

func TestDefaultEnvelopeFields(t *testing.T) {
	fields := DefaultEnvelopeFields()

	expected := map[string]bool{
		"_event_id":    true,
		"_event_type":  true,
		"_occurred_at": true,
		"_ingested_at": true,
		"_schema_id":   true,
		"_lsn":         true,
	}

	if len(fields) != len(expected) {
		t.Errorf("expected %d fields, got %d", len(expected), len(fields))
	}

	for _, f := range fields {
		if !expected[f.Name] {
			t.Errorf("unexpected field: %s", f.Name)
		}
	}
}
