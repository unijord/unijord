package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	// ErrInvalidPipeline is returned when x-pipeline is malformed.
	ErrInvalidPipeline = errors.New("invalid x-pipeline configuration")

	// ErrInvalidSchemaJSON is returned when schema is not valid JSON.
	ErrInvalidSchemaJSON = errors.New("invalid schema JSON")
)

// ExtractSchemaWithPipeline parses schema JSON and returns both
// the cleaned schema (without x-pipeline) and the pipeline config.
func ExtractSchemaWithPipeline(schemaJSON []byte) (cleanedSchema []byte, config *Config, err error) {
	var schemaMap map[string]any
	if err := json.Unmarshal(schemaJSON, &schemaMap); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrInvalidSchemaJSON, err)
	}

	if pipelineRaw, ok := schemaMap["x-pipeline"]; ok {
		pipelineJSON, err := json.Marshal(pipelineRaw)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: failed to marshal x-pipeline: %v", ErrInvalidPipeline, err)
		}

		config = &Config{}
		if err := json.Unmarshal(pipelineJSON, config); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrInvalidPipeline, err)
		}

		if err := config.Validate(); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrInvalidPipeline, err)
		}
	}

	delete(schemaMap, "x-pipeline")
	cleanedSchema, err = json.Marshal(schemaMap)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal cleaned schema: %w", err)
	}

	return cleanedSchema, config, nil
}
