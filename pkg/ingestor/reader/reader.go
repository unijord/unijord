package reader

// Reader decodes event payloads to map values.
type Reader interface {
	// Read decodes a single event payload using its schema.
	Read(schemaID uint32, payload []byte) (map[string]any, error)
}
