package reader

import (
	"fmt"

	"github.com/unijord/unijord/pkg/ingestor/schemaregistry"
)

// AvroReader decodes Avro payloads using the schema registry.
type AvroReader struct {
	registry *schemaregistry.Registry
}

// NewAvroReader creates a new Avro reader.
func NewAvroReader(registry *schemaregistry.Registry) *AvroReader {
	return &AvroReader{
		registry: registry,
	}
}

// Read decodes an Avro payload using the schema.
func (r *AvroReader) Read(schemaID uint32, payload []byte) (map[string]any, error) {
	validator, err := r.registry.GetValidator(schemaID)
	if err != nil {
		return nil, fmt.Errorf("get validator for schema %d: %w", schemaID, err)
	}

	var record map[string]any
	if err := validator.Decode(payload, &record); err != nil {
		return nil, fmt.Errorf("decode avro payload: %w", err)
	}

	return record, nil
}
