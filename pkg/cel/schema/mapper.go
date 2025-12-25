package schema

// MappedSchema holds the parsed schema for CEL environment creation.
// The Raw field contains the original parsed schema (e.g., avro.Schema)
type MappedSchema struct {
	Raw any
}

// Mapper parses schema bytes into a MappedSchema.
type Mapper interface {
	Map(schema []byte) (*MappedSchema, error)
}
