package schemaregistry

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hamba/avro/v2"
)

func TestRegistry_RegisterAndGetAVRO(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "record",
		"name": "Order",
		"fields": [
			{"name": "order_id", "type": "string"},
			{"name": "amount", "type": "long"}
		]
	}`

	err = reg.Register(100, SchemaTypeAVRO, schemaJSON)
	if err != nil {
		t.Fatalf("RegisterAVRO: %v", err)
	}

	schema, err := reg.Get(100)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if schema.ID != 100 {
		t.Errorf("schema ID mismatch: expected 100, got %d", schema.ID)
	}
	if schema.Type != SchemaTypeAVRO {
		t.Errorf("expected AVRO type, got %v", schema.Type)
	}
	if schema.Validator == nil {
		t.Error("validator should not be nil")
	}

	schema2JSON := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "user_id", "type": "string"},
			{"name": "email", "type": "string"}
		]
	}`
	err = reg.Register(200, SchemaTypeAVRO, schema2JSON)
	if err != nil {
		t.Fatalf("Register second: %v", err)
	}

	schema2, err := reg.Get(200)
	if err != nil {
		t.Fatalf("Get second: %v", err)
	}
	if schema2.ID != 200 {
		t.Errorf("expected id 200, got %d", schema2.ID)
	}
}

func TestRegistry_RegisterAndGetJSON(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "integer"}
		},
		"required": ["name", "age"]
	}`

	err = reg.Register(50, SchemaTypeJSON, schemaJSON)
	if err != nil {
		t.Fatalf("RegisterJSON: %v", err)
	}

	schema, err := reg.Get(50)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if schema.ID != 50 {
		t.Errorf("schema ID mismatch: expected 50, got %d", schema.ID)
	}
	if schema.Type != SchemaTypeJSON {
		t.Errorf("expected JSON type, got %v", schema.Type)
	}
	if schema.Validator == nil {
		t.Error("validator should not be nil")
	}
}

func TestRegistry_MixedSchemaTypes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	avroSchema := `{
		"type": "record",
		"name": "Event",
		"fields": [{"name": "id", "type": "string"}]
	}`
	err = reg.Register(1, SchemaTypeAVRO, avroSchema)
	if err != nil {
		t.Fatalf("RegisterAVRO: %v", err)
	}

	jsonSchema := `{
		"type": "object",
		"required": ["user_id"]
	}`
	err = reg.Register(2, SchemaTypeJSON, jsonSchema)
	if err != nil {
		t.Fatalf("RegisterJSON: %v", err)
	}

	s1, err := reg.Get(1)
	if err != nil {
		t.Fatalf("Get AVRO: %v", err)
	}
	if s1.Type != SchemaTypeAVRO {
		t.Errorf("expected AVRO, got %v", s1.Type)
	}

	s2, err := reg.Get(2)
	if err != nil {
		t.Fatalf("Get JSON: %v", err)
	}
	if s2.Type != SchemaTypeJSON {
		t.Errorf("expected JSON, got %v", s2.Type)
	}
}

func TestRegistry_DuplicateID(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "record",
		"name": "Test",
		"fields": [{"name": "id", "type": "string"}]
	}`

	err = reg.Register(1, SchemaTypeAVRO, schemaJSON)
	if err != nil {
		t.Fatalf("RegisterAVRO: %v", err)
	}

	err = reg.Register(1, SchemaTypeAVRO, schemaJSON)
	if !errors.Is(err, ErrSchemaExists) {
		t.Errorf("expected ErrSchemaExists, got %v", err)
	}
}

func TestRegistry_GetNotFound(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	_, err = reg.Get(999)
	if !errors.Is(err, ErrSchemaNotFound) {
		t.Errorf("expected ErrSchemaNotFound, got %v", err)
	}
}

func TestRegistry_InvalidAVROSchema(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	err = reg.Register(1, SchemaTypeAVRO, "not valid json")
	if err == nil {
		t.Error("expected error for invalid schema")
	}

	err = reg.Register(2, SchemaTypeAVRO, `{"type": "invalid"}`)
	if err == nil {
		t.Error("expected error for invalid avro type")
	}
}

func TestRegistry_InvalidJSONSchema(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	err = reg.Register(1, SchemaTypeJSON, "not valid json")
	if err == nil {
		t.Error("expected error for invalid JSON schema")
	}
}

func TestRegistry_Persistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	avroSchema := `{
		"type": "record",
		"name": "Event",
		"fields": [
			{"name": "id", "type": "string"}
		]
	}`

	jsonSchema := `{
		"type": "object",
		"required": ["id"]
	}`

	reg1, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry 1: %v", err)
	}
	err = reg1.Register(10, SchemaTypeAVRO, avroSchema)
	if err != nil {
		t.Fatalf("RegisterAVRO: %v", err)
	}
	err = reg1.Register(20, SchemaTypeJSON, jsonSchema)
	if err != nil {
		t.Fatalf("RegisterJSON: %v", err)
	}
	reg1.Close()

	reg2, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry 2: %v", err)
	}
	defer reg2.Close()

	s1, err := reg2.Get(10)
	if err != nil {
		t.Fatalf("Get AVRO after reopen: %v", err)
	}
	if s1.Type != SchemaTypeAVRO {
		t.Errorf("expected AVRO type after reopen")
	}
	if s1.ID != 10 {
		t.Errorf("expected ID 10, got %d", s1.ID)
	}

	s2, err := reg2.Get(20)
	if err != nil {
		t.Fatalf("Get JSON after reopen: %v", err)
	}
	if s2.Type != SchemaTypeJSON {
		t.Errorf("expected JSON type after reopen")
	}
	if s2.ID != 20 {
		t.Errorf("expected ID 20, got %d", s2.ID)
	}

	err = reg2.Register(30, SchemaTypeAVRO, `{"type": "record", "name": "Another", "fields": [{"name": "x", "type": "int"}]}`)
	if err != nil {
		t.Fatalf("Register after reopen: %v", err)
	}
}

func TestAVROValidator_Decode(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "record",
		"name": "Test",
		"fields": [
			{"name": "value", "type": "string"}
		]
	}`

	err = reg.Register(1, SchemaTypeAVRO, schemaJSON)
	if err != nil {
		t.Fatalf("RegisterAVRO: %v", err)
	}

	validator, err := reg.GetValidator(1)
	if err != nil {
		t.Fatalf("GetValidator: %v", err)
	}

	schema, _ := avro.Parse(schemaJSON)
	record := map[string]interface{}{"value": "hello"}
	binary, err := avro.Marshal(schema, record)
	if err != nil {
		t.Fatalf("avro.Marshal: %v", err)
	}

	var decoded map[string]interface{}
	err = validator.Decode(binary, &decoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded["value"] != "hello" {
		t.Errorf("decoded value mismatch: got %v", decoded["value"])
	}
}

func TestJSONValidator_Decode(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"count": {"type": "integer"}
		},
		"required": ["name", "count"]
	}`

	err = reg.Register(1, SchemaTypeJSON, schemaJSON)
	if err != nil {
		t.Fatalf("RegisterJSON: %v", err)
	}

	validator, err := reg.GetValidator(1)
	if err != nil {
		t.Fatalf("GetValidator: %v", err)
	}

	record := map[string]interface{}{"name": "test", "count": 42}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	var decoded map[string]interface{}
	err = validator.Decode(data, &decoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded["name"] != "test" {
		t.Errorf("decoded name mismatch: got %v", decoded["name"])
	}
}

func TestJSONValidator_RequiredFields(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "object",
		"required": ["name", "id"]
	}`

	err = reg.Register(1, SchemaTypeJSON, schemaJSON)
	if err != nil {
		t.Fatalf("RegisterJSON: %v", err)
	}

	validator, err := reg.GetValidator(1)
	if err != nil {
		t.Fatalf("GetValidator: %v", err)
	}

	validData := []byte(`{"name": "test", "id": "123"}`)
	var decoded map[string]interface{}
	err = validator.Decode(validData, &decoded)
	if err != nil {
		t.Errorf("expected valid data to pass: %v", err)
	}

	invalidData := []byte(`{"name": "test"}`)
	err = validator.Decode(invalidData, &decoded)
	if err == nil {
		t.Error("expected error for missing required field")
	}
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	err = reg.Register(1, SchemaTypeAVRO, `{
		"type": "record",
		"name": "Test",
		"fields": [{"name": "x", "type": "int"}]
	}`)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_, err := reg.Get(1)
				if err != nil {
					t.Errorf("concurrent Get failed: %v", err)
				}
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestSchemaType_String(t *testing.T) {
	tests := []struct {
		typ      SchemaType
		expected string
	}{
		{SchemaTypeAVRO, "AVRO"},
		{SchemaTypeJSON, "JSON"},
		{SchemaType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.expected {
			t.Errorf("SchemaType(%d).String() = %s, want %s", tt.typ, got, tt.expected)
		}
	}
}

func TestAvroValidator_RequiredFieldEnforcement(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": ["null", "string"], "default": null}
		]
	}`

	err = reg.Register(1, SchemaTypeAVRO, schemaJSON)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	schema, _ := reg.Get(1)
	validator := schema.Validator
	avroSchema, _ := avro.Parse(schemaJSON)

	validData := map[string]interface{}{
		"id":    "user-123",
		"name":  "John",
		"email": map[string]interface{}{"string": "john@example.com"},
	}
	encoded, err := avro.Marshal(avroSchema, validData)
	if err != nil {
		t.Fatalf("avro.Marshal valid data: %v", err)
	}
	var decoded map[string]interface{}
	if err := validator.Decode(encoded, &decoded); err != nil {
		t.Errorf("Decode valid data should pass: %v", err)
	}

	dataWithNullEmail := map[string]interface{}{
		"id":    "user-456",
		"name":  "Jane",
		"email": nil,
	}
	encoded, err = avro.Marshal(avroSchema, dataWithNullEmail)
	if err != nil {
		t.Fatalf("avro.Marshal data with null email: %v", err)
	}
	if err := validator.Decode(encoded, &decoded); err != nil {
		t.Errorf("Decode data with null email should pass: %v", err)
	}

	missingRequired := map[string]interface{}{
		"id": "user-789",

		"email": nil,
	}
	_, err = avro.Marshal(avroSchema, missingRequired)
	if err == nil {
		t.Error("avro.Marshal with missing required field should fail")
	}

	truncatedData := encoded[:len(encoded)/2]
	if err := validator.Decode(truncatedData, &decoded); err == nil {
		t.Error("Decode truncated data should fail")
	}
}

func TestJSONValidator_FullSchemaValidation(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"count": {"type": "integer"},
			"active": {"type": "boolean"}
		},
		"required": ["name", "count"]
	}`

	err = reg.Register(1, SchemaTypeJSON, schemaJSON)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	schema, _ := reg.Get(1)
	validator := schema.Validator
	var decoded map[string]interface{}

	validData := []byte(`{"name": "test", "count": 42, "active": true}`)
	if err := validator.Decode(validData, &decoded); err != nil {
		t.Errorf("Valid data should pass: %v", err)
	}

	missingRequired := []byte(`{"name": "test"}`)
	if err := validator.Decode(missingRequired, &decoded); err == nil {
		t.Error("Missing required field should fail")
	}

	wrongType := []byte(`{"name": "test", "count": "not-a-number"}`)
	if err := validator.Decode(wrongType, &decoded); err == nil {
		t.Error("Wrong type should fail validation")
	}

	wrongType2 := []byte(`{"name": 12345, "count": 42}`)
	if err := validator.Decode(wrongType2, &decoded); err == nil {
		t.Error("Wrong type (number for string) should fail validation")
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
