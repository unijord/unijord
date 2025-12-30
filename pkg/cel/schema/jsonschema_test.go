package schema

import (
	"errors"
	"testing"
)

func TestJsonSchemaMapper_ValidSchema(t *testing.T) {
	schema := `{
		"$id": "Order",
		"type": "object",
		"properties": {
			"order_id": {"type": "string"},
			"amount": {"type": "number"},
			"quantity": {"type": "integer"}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, err := m.Map([]byte(schema))
	if err != nil {
		t.Fatalf("Map error: %v", err)
	}

	if mapped.Raw == nil {
		t.Fatal("Raw should not be nil")
	}
}

func TestJsonSchemaMapper_InvalidSchema(t *testing.T) {
	m := NewJsonSchemaMapper()
	_, err := m.Map([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
	_, err = m.Map([]byte(`{"type": "string"}`))
	if err == nil {
		t.Error("expected error for non-object schema")
	}
}

func TestJsonSchemaAdapter_SimpleTypes(t *testing.T) {
	schema := `{
		"$id": "Test",
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "integer"},
			"score": {"type": "number"},
			"active": {"type": "boolean"}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	env, rootType, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	if rootType != "Test" {
		t.Errorf("rootType = %s, want Test", rootType)
	}

	valid := []string{
		"event.name == 'John'",
		"event.age > 18",
		"event.score > 90.5",
		"event.active == true",
	}

	for _, expr := range valid {
		_, issues := env.Compile(expr)
		if issues != nil && issues.Err() != nil {
			t.Errorf("expected valid: %s\n  error: %v", expr, issues.Err())
		}
	}

	invalid := []string{
		"event.nameeee == 'John'",
		"event.age > 'eighteen'",
		"event.score > 'ninety'",
		"event.nonexistent > 0",
	}

	for _, expr := range invalid {
		_, issues := env.Compile(expr)
		if issues == nil || issues.Err() == nil {
			t.Errorf("expected compile error for: %s", expr)
		}
	}
}

func TestJsonSchemaAdapter_NestedObjects(t *testing.T) {
	schema := `{
		"$id": "Order",
		"type": "object",
		"properties": {
			"order_id": {"type": "string"},
			"customer": {
				"type": "object",
				"title": "Customer",
				"properties": {
					"name": {"type": "string"},
					"age": {"type": "integer"},
					"address": {
						"type": "object",
						"title": "Address",
						"properties": {
							"city": {"type": "string"},
							"zip": {"type": "string"}
						}
					}
				}
			}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	env, _, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	valid := []string{
		"event.order_id == 'ORD-123'",
		"event.customer.name == 'John'",
		"event.customer.age > 18",
		"event.customer.address.city == 'NYC'",
		"event.customer.address.zip == '10001'",
	}

	for _, expr := range valid {
		_, issues := env.Compile(expr)
		if issues != nil && issues.Err() != nil {
			t.Errorf("expected valid: %s\n  error: %v", expr, issues.Err())
		}
	}

	invalid := []string{
		"event.customer.nameeee == 'John'",
		"event.customer.address.cityyy == 'NYC'",
	}

	for _, expr := range invalid {
		_, issues := env.Compile(expr)
		if issues == nil || issues.Err() == nil {
			t.Errorf("expected compile error for: %s", expr)
		}
	}
}

func TestJsonSchemaAdapter_Arrays(t *testing.T) {
	schema := `{
		"$id": "Order",
		"type": "object",
		"properties": {
			"tags": {
				"type": "array",
				"items": {"type": "string"}
			},
			"items": {
				"type": "array",
				"items": {
					"type": "object",
					"title": "Item",
					"properties": {
						"sku": {"type": "string"},
						"price": {"type": "number"},
						"qty": {"type": "integer"}
					}
				}
			}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	env, _, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	valid := []string{
		"event.tags.size() > 0",
		"'urgent' in event.tags",
		"event.items.size() > 0",
		"event.items[0].sku == 'ABC'",
		"event.items[0].price > 10.0",
		"event.items.filter(i, i.price > 5.0).size() > 0",
	}

	for _, expr := range valid {
		_, issues := env.Compile(expr)
		if issues != nil && issues.Err() != nil {
			t.Errorf("expected valid: %s\n  error: %v", expr, issues.Err())
		}
	}
}

func TestJsonSchemaAdapter_Maps(t *testing.T) {
	schema := `{
		"$id": "Event",
		"type": "object",
		"properties": {
			"metadata": {
				"type": "object",
				"additionalProperties": {"type": "string"}
			},
			"scores": {
				"type": "object",
				"additionalProperties": {"type": "number"}
			}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	env, _, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	valid := []string{
		"event.metadata['region'] == 'US'",
		"'region' in event.metadata",
		"event.scores['math'] > 90.0",
	}

	for _, expr := range valid {
		_, issues := env.Compile(expr)
		if issues != nil && issues.Err() != nil {
			t.Errorf("expected valid: %s\n  error: %v", expr, issues.Err())
		}
	}
}

func TestJsonSchemaAdapter_NullableTypes(t *testing.T) {
	schema := `{
		"$id": "Test",
		"type": "object",
		"properties": {
			"optional_name": {"type": ["string", "null"]},
			"optional_age": {"type": ["null", "integer"]}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	env, _, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	// Should be typed as string and int (nullable at runtime)
	valid := []string{
		"event.optional_name == 'John'",
		"event.optional_age > 18",
	}

	for _, expr := range valid {
		_, issues := env.Compile(expr)
		if issues != nil && issues.Err() != nil {
			t.Errorf("expected valid: %s\n  error: %v", expr, issues.Err())
		}
	}
}

func TestJsonSchemaAdapter_Formats(t *testing.T) {
	schema := `{
		"$id": "Event",
		"type": "object",
		"properties": {
			"created_at": {"type": "string", "format": "date-time"},
			"event_date": {"type": "string", "format": "date"},
			"email": {"type": "string", "format": "email"},
			"id": {"type": "string", "format": "uuid"}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	env, _, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	_, issues := env.Compile("year(event.created_at) > 2020")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected created_at to be timestamp: %v", issues.Err())
	}

	_, issues = env.Compile("event.email.contains('@')")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected email to be string: %v", issues.Err())
	}
}

func TestJsonSchemaAdapter_Definitions(t *testing.T) {
	schema := `{
		"$id": "Order",
		"type": "object",
		"properties": {
			"customer": {"$ref": "#/definitions/Customer"}
		},
		"definitions": {
			"Customer": {
				"type": "object",
				"title": "Customer",
				"properties": {
					"name": {"type": "string"},
					"age": {"type": "integer"}
				}
			}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	env, _, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	valid := []string{
		"event.customer.name == 'John'",
		"event.customer.age > 18",
	}

	for _, expr := range valid {
		_, issues := env.Compile(expr)
		if issues != nil && issues.Err() != nil {
			t.Errorf("expected valid: %s\n  error: %v", expr, issues.Err())
		}
	}
}

func TestJsonSchemaAdapter_Defs(t *testing.T) {
	// Test $defs (draft 2019-09+)
	schema := `{
		"$id": "Order",
		"type": "object",
		"properties": {
			"customer": {"$ref": "#/$defs/Customer"}
		},
		"$defs": {
			"Customer": {
				"type": "object",
				"title": "Customer",
				"properties": {
					"name": {"type": "string"}
				}
			}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	env, _, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	_, issues := env.Compile("event.customer.name == 'John'")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected valid expression: %v", issues.Err())
	}
}

func TestJsonSchemaAdapter_NoID(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"value": {"type": "integer"}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	_, rootType, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	if rootType != "Root" {
		t.Errorf("rootType = %s, want Root", rootType)
	}
}

func TestNewJsonSchemaAdapter_Errors(t *testing.T) {
	_, err := NewJsonSchemaAdapter(nil)
	if !errors.Is(err, ErrNilSchema) {
		t.Errorf("expected ErrNilSchema, got %v", err)
	}

	badSchema := &MappedSchema{Raw: "not a jsonSchemaNode"}
	_, err = NewJsonSchemaAdapter(badSchema)
	if err == nil {
		t.Error("expected error for wrong type")
	}
}

func TestJsonSchemaMapper_Validation(t *testing.T) {
	m := NewJsonSchemaMapper()

	valid := `{
		"type": "object",
		"properties": {
			"name": {"type": "string"}
		}
	}`
	_, err := m.Map([]byte(valid))
	if err != nil {
		t.Errorf("expected valid schema to pass: %v", err)
	}

	invalidRef := `{
		"type": "object",
		"properties": {
			"customer": {"$ref": "#/definitions/NonExistent"}
		}
	}`
	_, err = m.Map([]byte(invalidRef))
	if err == nil {
		t.Error("expected error for invalid $ref")
	}
}

func TestJsonSchemaMapper_SkipValidation(t *testing.T) {
	m := &JsonSchemaMapper{SkipValidation: true}
	invalidRef := `{
		"type": "object",
		"properties": {
			"customer": {"$ref": "#/definitions/NonExistent"}
		}
	}`
	mapped, err := m.Map([]byte(invalidRef))
	if err != nil {
		t.Errorf("with SkipValidation, should not fail: %v", err)
	}

	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()
	_, err = adapter.BuildTypes(provider)
	if err != nil {
		t.Errorf("BuildTypes error: %v", err)
	}
}

func TestJsonSchemaAdapter_Enum(t *testing.T) {
	schema := `{
		"$id": "Order",
		"type": "object",
		"properties": {
			"status": {
				"enum": ["pending", "active", "completed"]
			},
			"priority": {
				"enum": [1, 2, 3]
			}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	env, _, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	_, issues := env.Compile("event.status == 'pending'")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected string enum to work: %v", issues.Err())
	}

	_, issues = env.Compile("event.priority > 1")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected number enum to work: %v", issues.Err())
	}
	_, issues = env.Compile("event.status > 100")
	if issues == nil || issues.Err() == nil {
		t.Error("expected type error for string enum compared to int")
	}
}

func TestJsonSchemaAdapter_Const(t *testing.T) {
	schema := `{
		"$id": "Event",
		"type": "object",
		"properties": {
			"version": {"const": "1.0"},
			"code": {"const": 42}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	env, _, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	_, issues := env.Compile("event.version == '1.0'")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected string const to work: %v", issues.Err())
	}

	_, issues = env.Compile("event.code == 42")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected number const to work: %v", issues.Err())
	}
}

func TestJsonSchemaAdapter_AllOf(t *testing.T) {
	schema := `{
		"$id": "Employee",
		"type": "object",
		"properties": {
			"person": {
				"allOf": [
					{
						"type": "object",
						"properties": {
							"name": {"type": "string"},
							"age": {"type": "integer"}
						}
					},
					{
						"type": "object",
						"properties": {
							"email": {"type": "string"},
							"department": {"type": "string"}
						}
					}
				]
			}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	env, _, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	_, issues := env.Compile("event.person.name == 'John'")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected name field from allOf: %v", issues.Err())
	}

	_, issues = env.Compile("event.person.age > 18")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected age field from allOf: %v", issues.Err())
	}

	_, issues = env.Compile("event.person.email.contains('@')")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected email field from allOf: %v", issues.Err())
	}

	_, issues = env.Compile("event.person.department == 'Engineering'")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected department field from allOf: %v", issues.Err())
	}
}

func TestJsonSchemaAdapter_AllOfWithRef(t *testing.T) {
	schema := `{
		"$id": "Order",
		"type": "object",
		"properties": {
			"customer": {
				"allOf": [
					{"$ref": "#/definitions/Person"},
					{
						"type": "object",
						"properties": {
							"vip": {"type": "boolean"}
						}
					}
				]
			}
		},
		"definitions": {
			"Person": {
				"type": "object",
				"properties": {
					"name": {"type": "string"},
					"age": {"type": "integer"}
				}
			}
		}
	}`

	m := NewJsonSchemaMapper()
	mapped, _ := m.Map([]byte(schema))
	adapter, _ := NewJsonSchemaAdapter(mapped)
	provider := NewTypeProvider()

	env, _, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	_, issues := env.Compile("event.customer.name == 'John'")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected name from $ref in allOf: %v", issues.Err())
	}

	_, issues = env.Compile("event.customer.vip == true")
	if issues != nil && issues.Err() != nil {
		t.Errorf("expected vip from inline schema in allOf: %v", issues.Err())
	}
}
