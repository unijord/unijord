package reader

import (
	"fmt"

	"github.com/unijord/unijord/pkg/ingestor/schemaregistry"
)

// JSONReader decodes JSON payloads using the schema registry.
type JSONReader struct {
	registry *schemaregistry.Registry
}

// NewJSONReader creates a new JSON reader.
func NewJSONReader(registry *schemaregistry.Registry) *JSONReader {
	return &JSONReader{
		registry: registry,
	}
}

// Read decodes a JSON payload using the schema.
func (r *JSONReader) Read(schemaID uint32, payload []byte) (map[string]any, error) {
	validator, err := r.registry.GetValidator(schemaID)
	if err != nil {
		return nil, fmt.Errorf("get validator for schema %d: %w", schemaID, err)
	}

	var record map[string]any
	if err := validator.Decode(payload, &record); err != nil {
		return nil, fmt.Errorf("decode json payload: %w", err)
	}

	return record, nil
}
