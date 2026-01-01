package pipeline

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestExtractSchemaWithPipeline(t *testing.T) {
	schemaJSON := []byte(`{
		"type": "record",
		"name": "Order",
		"fields": [
			{"name": "order_id", "type": "string"},
			{"name": "amount", "type": "double"}
		],
		"x-pipeline": {
			"validate": ["event.amount > 0"],
			"filter": "event.amount >= 10",
			"columns": [
				{"name": "order_id", "expr": "event.order_id", "field_id": 1},
				{"name": "amount", "expr": "event.amount", "field_id": 2},
				{"name": "is_large", "expr": "event.amount > 100", "field_id": 3}
			],
			"partition_spec": [
				{"source_id": 1, "field_id": 1000, "transform": "identity", "name": "order_id"}
			],
			"sort_order": [
				{"source_id": 2, "transform": "identity", "direction": "asc", "null_order": "nulls-last"}
			]
		}
	}`)

	cleanSchema, config, err := ExtractSchemaWithPipeline(schemaJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if config == nil {
		t.Fatal("expected config, got nil")
	}

	if len(config.Validations) != 1 || config.Validations[0] != "event.amount > 0" {
		t.Errorf("unexpected validations: %v", config.Validations)
	}

	if config.Filter != "event.amount >= 10" {
		t.Errorf("unexpected filter: %s", config.Filter)
	}

	if len(config.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(config.Columns))
	}
	if config.Columns[0].Name != "order_id" || config.Columns[0].Expr != "event.order_id" {
		t.Errorf("unexpected column[0]: %+v", config.Columns[0])
	}

	if len(config.PartitionSpec) != 1 {
		t.Errorf("expected 1 partition field, got %d", len(config.PartitionSpec))
	} else {
		pf := config.PartitionSpec[0]
		if pf.SourceID != 1 || pf.FieldID != 1000 || pf.Transform != TransformIdentity || pf.Name != "order_id" {
			t.Errorf("unexpected partition_spec[0]: %+v", pf)
		}
	}

	if len(config.SortOrder) != 1 {
		t.Errorf("expected 1 sort field, got %d", len(config.SortOrder))
	} else {
		sf := config.SortOrder[0]
		if sf.SourceID != 2 || sf.Transform != TransformIdentity || sf.Direction != SortAsc || sf.NullOrder != NullsLast {
			t.Errorf("unexpected sort_order[0]: %+v", sf)
		}
	}

	var schemaMap map[string]any
	if err := json.Unmarshal(cleanSchema, &schemaMap); err != nil {
		t.Fatalf("failed to parse cleaned schema: %v", err)
	}
	if _, exists := schemaMap["x-pipeline"]; exists {
		t.Error("x-pipeline should be removed from cleaned schema")
	}

	if schemaMap["type"] != "record" {
		t.Error("type should be preserved")
	}
	if schemaMap["name"] != "Order" {
		t.Error("name should be preserved")
	}
	if schemaMap["fields"] == nil {
		t.Error("fields should be preserved")
	}
}

func TestExtractSchemaWithPipeline_NoPipeline(t *testing.T) {
	schemaJSON := []byte(`{
		"type": "record",
		"name": "Order",
		"fields": [{"name": "id", "type": "string"}]
	}`)

	cleanSchema, config, err := ExtractSchemaWithPipeline(schemaJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if config != nil {
		t.Error("expected nil config for schema without x-pipeline")
	}

	var schemaMap map[string]any
	if err := json.Unmarshal(cleanSchema, &schemaMap); err != nil {
		t.Fatalf("failed to parse cleaned schema: %v", err)
	}
	if schemaMap["type"] != "record" {
		t.Error("type should be preserved")
	}
}

func TestExtractSchemaWithPipeline_InvalidJSON(t *testing.T) {
	schemaJSON := []byte(`{invalid json}`)

	_, _, err := ExtractSchemaWithPipeline(schemaJSON)
	if !errors.Is(err, ErrInvalidSchemaJSON) {
		t.Errorf("expected ErrInvalidSchemaJSON, got: %v", err)
	}
}

func TestExtractSchemaWithPipeline_InvalidPipeline(t *testing.T) {
	schemaJSON := []byte(`{
		"type": "record",
		"x-pipeline": {
			"columns": []
		}
	}`)

	_, _, err := ExtractSchemaWithPipeline(schemaJSON)
	if !errors.Is(err, ErrInvalidPipeline) {
		t.Errorf("expected ErrInvalidPipeline, got: %v", err)
	}
}

func TestExtractSchemaWithPipeline_MinimalPipeline(t *testing.T) {
	schemaJSON := []byte(`{
		"type": "record",
		"name": "Event",
		"x-pipeline": {
			"columns": [
				{"name": "id", "expr": "event.id"}
			]
		}
	}`)

	cleanSchema, config, err := ExtractSchemaWithPipeline(schemaJSON)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if config == nil {
		t.Fatal("expected config, got nil")
	}
	if len(config.Columns) != 1 {
		t.Errorf("expected 1 column, got %d", len(config.Columns))
	}

	var schemaMap map[string]any
	if err := json.Unmarshal(cleanSchema, &schemaMap); err != nil {
		t.Fatalf("failed to parse cleaned schema: %v", err)
	}
	if _, exists := schemaMap["x-pipeline"]; exists {
		t.Error("x-pipeline should be removed")
	}
}
