package schema

import (
	"github.com/google/cel-go/cel"
)

// SchemaAdapter converts a parsed schema into CEL types.
type SchemaAdapter interface {
	// BuildTypes registers all types from the schema with the provider.
	// Returns the root type name (e.g., "com.example.Order").
	BuildTypes(provider *TypeProvider) (rootTypeName string, err error)
}

// EnvOptions configures the CEL environment creation.
type EnvOptions struct {
	// EnvelopeFields are additional variables available in expressions.
	// Default envelope fields (_event_id, _event_type, etc.) are always included.
	// Use this to add custom metadata fields.
	EnvelopeFields []EnvelopeField

	// DisableDefaultEnvelope disables the default envelope fields.
	DisableDefaultEnvelope bool

	// EventVarName is the name of the event variable (default: "event").
	EventVarName string

	// AdditionalOpts are extra CEL environment options.
	AdditionalOpts []cel.EnvOption
}

// EnvelopeField defines a variable available in CEL expressions.
type EnvelopeField struct {
	Name string
	Type *cel.Type
}

// DefaultEnvelopeFields returns the standard envelope fields.
func DefaultEnvelopeFields() []EnvelopeField {
	return []EnvelopeField{
		{Name: "_event_id", Type: cel.BytesType},
		{Name: "_event_type", Type: cel.StringType},
		{Name: "_occurred_at", Type: cel.TimestampType},
		{Name: "_ingested_at", Type: cel.TimestampType},
		{Name: "_schema_id", Type: cel.IntType},
		{Name: "_lsn", Type: cel.IntType},
	}
}

// DefaultEnvOptions returns sensible defaults.
func DefaultEnvOptions() EnvOptions {
	return EnvOptions{
		EventVarName: "event",
	}
}
