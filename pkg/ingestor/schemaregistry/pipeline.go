package schemaregistry

import (
	"fmt"

	celschema "github.com/unijord/unijord/pkg/cel/schema"
	"github.com/unijord/unijord/pkg/pipeline"
)

func compilePipeline(
	schemaType SchemaType,
	schemaJSON []byte,
	config *pipeline.Config,
) (*pipeline.CompiledPipeline, error) {
	var mapped *celschema.MappedSchema
	var err error

	switch schemaType {
	case SchemaTypeAVRO:
		mapper := celschema.NewAvroMapper()
		mapped, err = mapper.Map(schemaJSON)
		if err != nil {
			return nil, fmt.Errorf("map avro schema: %w", err)
		}

	case SchemaTypeJSON:
		mapper := celschema.NewJsonSchemaMapper()
		mapped, err = mapper.Map(schemaJSON)
		if err != nil {
			return nil, fmt.Errorf("map json schema: %w", err)
		}

	default:
		return nil, fmt.Errorf("unsupported schema type for pipeline: %v", schemaType)
	}

	compiler := pipeline.NewCompiler()
	compiled, err := compiler.Compile(config, mapped, "")
	if err != nil {
		return nil, fmt.Errorf("compile pipeline: %w", err)
	}

	return compiled, nil
}
