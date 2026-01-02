package reader

import (
	"path/filepath"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/unijord/unijord/pkg/ingestor/schemaregistry"
)

func TestAvroReader_Read(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := schemaregistry.NewRegistry(dbPath)
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

	err = reg.Register(1, schemaregistry.SchemaTypeAVRO, schemaJSON)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	avroSchema, _ := avro.Parse(schemaJSON)
	record := map[string]interface{}{
		"order_id": "order-123",
		"amount":   int64(9999),
	}
	payload, err := avro.Marshal(avroSchema, record)
	if err != nil {
		t.Fatalf("avro.Marshal: %v", err)
	}

	reader := NewAvroReader(reg)
	decoded, err := reader.Read(1, payload)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if decoded["order_id"] != "order-123" {
		t.Errorf("order_id: expected order-123, got %v", decoded["order_id"])
	}
	if decoded["amount"] != int64(9999) {
		t.Errorf("amount: expected 9999, got %v", decoded["amount"])
	}
}

func TestAvroReader_ReadNested(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := schemaregistry.NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "record",
		"name": "Order",
		"fields": [
			{"name": "id", "type": "string"},
			{
				"name": "address",
				"type": {
					"type": "record",
					"name": "Address",
					"fields": [
						{"name": "street", "type": "string"},
						{"name": "city", "type": "string"}
					]
				}
			}
		]
	}`

	err = reg.Register(1, schemaregistry.SchemaTypeAVRO, schemaJSON)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	avroSchema, _ := avro.Parse(schemaJSON)
	record := map[string]interface{}{
		"id": "order-1",
		"address": map[string]interface{}{
			"street": "123 Main St",
			"city":   "Springfield",
		},
	}
	payload, _ := avro.Marshal(avroSchema, record)

	reader := NewAvroReader(reg)
	decoded, err := reader.Read(1, payload)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if decoded["id"] != "order-1" {
		t.Errorf("id: expected order-1, got %v", decoded["id"])
	}

	addr, ok := decoded["address"].(map[string]interface{})
	if !ok {
		t.Fatalf("address: expected map, got %T", decoded["address"])
	}
	if addr["city"] != "Springfield" {
		t.Errorf("city: expected Springfield, got %v", addr["city"])
	}
}

func TestAvroReader_InvalidSchema(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := schemaregistry.NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	reader := NewAvroReader(reg)
	_, err = reader.Read(999, []byte("data"))
	if err == nil {
		t.Error("expected error for unknown schema")
	}
}

func TestJSONReader_Read(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := schemaregistry.NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "object",
		"properties": {
			"order_id": {"type": "string"},
			"amount": {"type": "integer"}
		},
		"required": ["order_id", "amount"]
	}`

	err = reg.Register(1, schemaregistry.SchemaTypeJSON, schemaJSON)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	payload := []byte(`{"order_id": "order-123", "amount": 9999}`)

	reader := NewJSONReader(reg)
	decoded, err := reader.Read(1, payload)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if decoded["order_id"] != "order-123" {
		t.Errorf("order_id: expected order-123, got %v", decoded["order_id"])
	}

	if decoded["amount"] != float64(9999) {
		t.Errorf("amount: expected 9999, got %v (%T)", decoded["amount"], decoded["amount"])
	}
}

func TestJSONReader_ReadNested(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := schemaregistry.NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "object",
		"properties": {
			"id": {"type": "string"},
			"address": {
				"type": "object",
				"properties": {
					"street": {"type": "string"},
					"city": {"type": "string"}
				}
			}
		},
		"required": ["id"]
	}`

	err = reg.Register(1, schemaregistry.SchemaTypeJSON, schemaJSON)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	payload := []byte(`{"id": "order-1", "address": {"street": "123 Main St", "city": "Springfield"}}`)

	reader := NewJSONReader(reg)
	decoded, err := reader.Read(1, payload)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if decoded["id"] != "order-1" {
		t.Errorf("id: expected order-1, got %v", decoded["id"])
	}

	addr, ok := decoded["address"].(map[string]interface{})
	if !ok {
		t.Fatalf("address: expected map, got %T", decoded["address"])
	}
	if addr["city"] != "Springfield" {
		t.Errorf("city: expected Springfield, got %v", addr["city"])
	}
}

func TestJSONReader_ValidationFailure(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := schemaregistry.NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "object",
		"properties": {
			"order_id": {"type": "string"},
			"amount": {"type": "integer"}
		},
		"required": ["order_id", "amount"]
	}`

	err = reg.Register(1, schemaregistry.SchemaTypeJSON, schemaJSON)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	payload := []byte(`{"order_id": "order-123"}`)

	reader := NewJSONReader(reg)
	_, err = reader.Read(1, payload)
	if err == nil {
		t.Error("expected validation error for missing required field")
	}
}

func TestSchemaReader_Avro(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := schemaregistry.NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "record",
		"name": "Order",
		"fields": [
			{"name": "order_id", "type": "string"}
		]
	}`

	err = reg.Register(1, schemaregistry.SchemaTypeAVRO, schemaJSON)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	avroSchema, _ := avro.Parse(schemaJSON)
	payload, _ := avro.Marshal(avroSchema, map[string]interface{}{"order_id": "test-123"})

	reader := NewSchemaReader(reg)
	decoded, err := reader.Read(1, payload)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if decoded["order_id"] != "test-123" {
		t.Errorf("order_id: expected test-123, got %v", decoded["order_id"])
	}
}

func TestSchemaReader_JSON(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := schemaregistry.NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "object",
		"properties": {
			"order_id": {"type": "string"}
		},
		"required": ["order_id"]
	}`

	err = reg.Register(1, schemaregistry.SchemaTypeJSON, schemaJSON)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	payload := []byte(`{"order_id": "test-456"}`)

	reader := NewSchemaReader(reg)
	decoded, err := reader.Read(1, payload)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if decoded["order_id"] != "test-456" {
		t.Errorf("order_id: expected test-456, got %v", decoded["order_id"])
	}
}

func TestSchemaReader_MixedSchemas(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := schemaregistry.NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	avroSchemaJSON := `{
		"type": "record",
		"name": "Order",
		"fields": [{"name": "id", "type": "string"}]
	}`
	err = reg.Register(1, schemaregistry.SchemaTypeAVRO, avroSchemaJSON)
	if err != nil {
		t.Fatalf("Register Avro: %v", err)
	}

	jsonSchemaJSON := `{
		"type": "object",
		"properties": {"id": {"type": "string"}},
		"required": ["id"]
	}`
	err = reg.Register(2, schemaregistry.SchemaTypeJSON, jsonSchemaJSON)
	if err != nil {
		t.Fatalf("Register JSON: %v", err)
	}

	reader := NewSchemaReader(reg)

	avroSchema, _ := avro.Parse(avroSchemaJSON)
	avroPayload, _ := avro.Marshal(avroSchema, map[string]interface{}{"id": "avro-1"})
	decoded1, err := reader.Read(1, avroPayload)
	if err != nil {
		t.Fatalf("Read Avro: %v", err)
	}
	if decoded1["id"] != "avro-1" {
		t.Errorf("Avro id: expected avro-1, got %v", decoded1["id"])
	}

	jsonPayload := []byte(`{"id": "json-2"}`)
	decoded2, err := reader.Read(2, jsonPayload)
	if err != nil {
		t.Fatalf("Read JSON: %v", err)
	}
	if decoded2["id"] != "json-2" {
		t.Errorf("JSON id: expected json-2, got %v", decoded2["id"])
	}
}

func TestSchemaReader_CachesSchemaType(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")

	reg, err := schemaregistry.NewRegistry(dbPath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	schemaJSON := `{
		"type": "record",
		"name": "Order",
		"fields": [{"name": "id", "type": "string"}]
	}`
	err = reg.Register(1, schemaregistry.SchemaTypeAVRO, schemaJSON)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	avroSchema, _ := avro.Parse(schemaJSON)
	payload, _ := avro.Marshal(avroSchema, map[string]interface{}{"id": "test"})

	reader := NewSchemaReader(reg)

	_, err = reader.Read(1, payload)
	if err != nil {
		t.Fatalf("First Read: %v", err)
	}

	reader.mu.RLock()
	_, cached := reader.schemaTypes[1]
	reader.mu.RUnlock()

	if !cached {
		t.Error("expected schema type to be cached")
	}

	_, err = reader.Read(1, payload)
	if err != nil {
		t.Fatalf("Second Read: %v", err)
	}
}
