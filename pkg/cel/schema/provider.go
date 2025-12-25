package schema

import (
	"errors"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/unijord/unijord/pkg/cel/ext"
)

var (
	// ErrNilSchema is returned when schema is nil.
	ErrNilSchema = errors.New("schema is nil")

	// ErrNotAvroSchema is returned when schema.Raw is not an avro.Schema.
	ErrNotAvroSchema = errors.New("schema is not an Avro schema")

	// ErrNotRecordSchema is returned when schema is not a record type.
	ErrNotRecordSchema = errors.New("schema must be a record type")

	// ErrNilAdapter is returned when adapter is nil.
	ErrNilAdapter = errors.New("adapter is nil")
)

// TypeProvider implements cel.TypeProvider for schema-driven type checking.
type TypeProvider struct {
	types map[string]map[string]*cel.Type
}

// NewTypeProvider creates an empty TypeProvider.
func NewTypeProvider() *TypeProvider {
	return &TypeProvider{
		types: make(map[string]map[string]*cel.Type),
	}
}

func (p *TypeProvider) EnumValue(enumName string) ref.Val {
	return types.NewErr("unknown enum: %s", enumName)
}

func (p *TypeProvider) FindIdent(identName string) (ref.Val, bool) {
	return nil, false
}

func (p *TypeProvider) FindStructType(structType string) (*types.Type, bool) {
	if _, ok := p.types[structType]; ok {
		return types.NewObjectType(structType), true
	}
	return nil, false
}

func (p *TypeProvider) FindStructFieldNames(structType string) ([]string, bool) {
	fields, ok := p.types[structType]
	if !ok {
		return nil, false
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	return names, true
}

func (p *TypeProvider) FindStructFieldType(structType, fieldName string) (*types.FieldType, bool) {
	fields, ok := p.types[structType]
	if !ok {
		return nil, false
	}
	ft, ok := fields[fieldName]
	if !ok {
		return nil, false
	}
	return &types.FieldType{Type: ft}, true
}

func (p *TypeProvider) NewValue(structType string, fields map[string]ref.Val) ref.Val {
	return types.NewErr("NewValue not implemented for %s", structType)
}

func (p *TypeProvider) registerType(name string, fields map[string]*cel.Type) {
	p.types[name] = fields
}

// BuildTypedEnv creates a CEL environment using a SchemaAdapter.
func BuildTypedEnv(adapter SchemaAdapter, provider *TypeProvider, opts EnvOptions) (*cel.Env, string, error) {
	if adapter == nil {
		return nil, "", ErrNilAdapter
	}

	rootType, err := adapter.BuildTypes(provider)
	if err != nil {
		return nil, "", err
	}

	envOpts := []cel.EnvOption{
		cel.CustomTypeProvider(provider),
	}

	envOpts = append(envOpts, ext.AllFuncs()...)
	if !opts.DisableDefaultEnvelope {
		for _, f := range DefaultEnvelopeFields() {
			envOpts = append(envOpts, cel.Variable(f.Name, f.Type))
		}
	}

	for _, f := range opts.EnvelopeFields {
		envOpts = append(envOpts, cel.Variable(f.Name, f.Type))
	}

	eventVar := opts.EventVarName
	if eventVar == "" {
		eventVar = "event"
	}
	envOpts = append(envOpts, cel.Variable(eventVar, cel.ObjectType(rootType)))

	envOpts = append(envOpts, opts.AdditionalOpts...)

	env, err := cel.NewEnv(envOpts...)
	return env, rootType, err
}
