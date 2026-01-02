package reader

import (
	"fmt"
	"sync"

	"github.com/unijord/unijord/pkg/ingestor/schemaregistry"
)

// SchemaReader is a unified reader that handles both Avro and JSON schemas.
type SchemaReader struct {
	registry   *schemaregistry.Registry
	avroReader *AvroReader
	jsonReader *JSONReader

	mu          sync.RWMutex
	schemaTypes map[uint32]schemaregistry.SchemaType
}

// NewSchemaReader creates a new unified schema reader.
func NewSchemaReader(registry *schemaregistry.Registry) *SchemaReader {
	return &SchemaReader{
		registry:    registry,
		avroReader:  NewAvroReader(registry),
		jsonReader:  NewJSONReader(registry),
		schemaTypes: make(map[uint32]schemaregistry.SchemaType),
	}
}

// Read decodes a payload using the appropriate reader based on schema type.
func (r *SchemaReader) Read(schemaID uint32, payload []byte) (map[string]any, error) {
	schemaType, err := r.getSchemaType(schemaID)
	if err != nil {
		return nil, err
	}

	switch schemaType {
	case schemaregistry.SchemaTypeAVRO:
		return r.avroReader.Read(schemaID, payload)
	case schemaregistry.SchemaTypeJSON:
		return r.jsonReader.Read(schemaID, payload)
	default:
		return nil, fmt.Errorf("unsupported schema type: %v", schemaType)
	}
}

func (r *SchemaReader) getSchemaType(schemaID uint32) (schemaregistry.SchemaType, error) {
	r.mu.RLock()
	if t, ok := r.schemaTypes[schemaID]; ok {
		r.mu.RUnlock()
		return t, nil
	}
	r.mu.RUnlock()

	schema, err := r.registry.Get(schemaID)
	if err != nil {
		return 0, fmt.Errorf("get schema %d: %w", schemaID, err)
	}

	r.mu.Lock()
	r.schemaTypes[schemaID] = schema.Type
	r.mu.Unlock()

	return schema.Type, nil
}
