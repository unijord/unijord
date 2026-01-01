package pipeline

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/cel-go/cel"
	celschema "github.com/unijord/unijord/pkg/cel/schema"
)

// Compiler compiles pipeline configurations against schemas.
type Compiler struct {
	envOptions []cel.EnvOption
}

// NewCompiler creates a new pipeline compiler.
func NewCompiler(opts ...CompilerOption) *Compiler {
	c := &Compiler{}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// CompilerOption configures the compiler.
type CompilerOption func(*Compiler)

// WithEnvOptions adds additional CEL environment options.
func WithEnvOptions(opts ...cel.EnvOption) CompilerOption {
	return func(c *Compiler) {
		c.envOptions = append(c.envOptions, opts...)
	}
}

// Compile compiles a pipeline configuration against a mapped schema.
func (c *Compiler) Compile(
	config *Config,
	mapped *celschema.MappedSchema,
	schemaName string,
) (*CompiledPipeline, error) {
	// Validation is done in CompileWithAdapter
	adapter, err := getAdapter(mapped, config.GetSchemaFormat())
	if err != nil {
		return nil, fmt.Errorf("failed to create schema adapter: %w", err)
	}

	return c.CompileWithAdapter(config, adapter, schemaName)
}

// CompileWithAdapter compiles using a schema adapter directly.
func (c *Compiler) CompileWithAdapter(
	config *Config,
	adapter celschema.SchemaAdapter,
	schemaName string,
) (*CompiledPipeline, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid pipeline config: %w", err)
	}

	provider := celschema.NewTypeProvider()
	envOpts := celschema.DefaultEnvOptions()
	envOpts.AdditionalOpts = append(envOpts.AdditionalOpts, c.envOptions...)
	env, rootType, err := celschema.BuildTypedEnv(adapter, provider, envOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to build CEL environment: %w", err)
	}

	if schemaName == "" {
		schemaName = rootType
	}

	return c.compileWithEnv(config, env, schemaName)
}

// compileWithEnv compiles using a pre-built CEL environment.
func (c *Compiler) compileWithEnv(
	config *Config,
	env *cel.Env,
	schemaName string,
) (*CompiledPipeline, error) {
	compileErrors := &CompileErrors{}

	// validation expr
	validations := make([]CompiledValidation, 0, len(config.Validations))
	for i, expr := range config.Validations {
		prog, err := compileExpression(env, expr, cel.BoolType)
		if err != nil {
			compileErrors.Add(CompileError{
				Location: fmt.Sprintf("validate[%d]", i),
				Source:   expr,
				Err:      err,
			})
			continue
		}
		validations = append(validations, CompiledValidation{
			Index:   i,
			Source:  expr,
			Program: prog,
		})
	}

	// filter expr
	var filter *CompiledFilter
	if config.Filter != "" {
		prog, err := compileExpression(env, config.Filter, cel.BoolType)
		if err != nil {
			compileErrors.Add(CompileError{
				Location: "filter",
				Source:   config.Filter,
				Err:      err,
			})
		} else {
			filter = &CompiledFilter{
				Source:  config.Filter,
				Program: prog,
			}
		}
	}

	// column expr
	columns := make([]CompiledColumn, 0, len(config.Columns))
	for i, col := range config.Columns {
		ast, issues := env.Compile(col.Expr)
		if issues != nil && issues.Err() != nil {
			compileErrors.Add(CompileError{
				Location: fmt.Sprintf("columns[%d].expr", i),
				Source:   col.Expr,
				Err:      issues.Err(),
			})
			continue
		}
		if ast == nil {
			compileErrors.Add(CompileError{
				Location: fmt.Sprintf("columns[%d].expr", i),
				Source:   col.Expr,
				Err:      fmt.Errorf("compilation produced nil AST"),
			})
			continue
		}

		prog, err := env.Program(ast)
		if err != nil {
			compileErrors.Add(CompileError{
				Location: fmt.Sprintf("columns[%d].expr", i),
				Source:   col.Expr,
				Err:      err,
			})
			continue
		}

		// CEL output type
		celType := ast.OutputType()

		// Determine Arrow type and Iceberg type
		// Explicit "as" override takes precedence
		var arrowType arrow.DataType
		var icebergType string
		if col.As != "" {
			var err error
			arrowType, err = ParseOutputType(col.As)
			if err != nil {
				compileErrors.Add(CompileError{
					Location: fmt.Sprintf("columns[%d].as", i),
					Source:   col.As,
					Err:      err,
				})
				continue
			}
			// Use "as" value directly as Iceberg type.
			icebergType = col.As
		} else {
			var err error
			arrowType, err = CELTypeToArrow(celType)
			if err != nil {
				compileErrors.Add(CompileError{
					Location: fmt.Sprintf("columns[%d].type", i),
					Source:   col.Expr,
					Err:      fmt.Errorf("cannot map CEL type %s to Arrow: %w", celType, err),
				})
				continue
			}
			// Infer Iceberg type from Arrow type
			icebergType = arrowTypeToIcebergType(arrowType)
		}

		columns = append(columns, CompiledColumn{
			Name:        col.Name,
			Index:       i,
			Source:      col.Expr,
			Program:     prog,
			CELType:     celType,
			ArrowType:   arrowType,
			IcebergType: icebergType,
			Nullable:    IsNullableCELType(celType),
			FieldID:     col.FieldID,
		})
	}

	if compileErrors.HasErrors() {
		return nil, compileErrors
	}

	// field_id to column index lookup
	fieldIDToIndex := make(map[int]int)
	for i, col := range columns {
		if col.FieldID != nil {
			fieldIDToIndex[*col.FieldID] = i
		}
	}

	// partition spec - map source_id to column index
	partitionSpec := make([]CompiledPartitionField, len(config.PartitionSpec))
	for i, pf := range config.PartitionSpec {
		colIndex, ok := fieldIDToIndex[pf.SourceID]
		if !ok {
			return nil, fmt.Errorf("partition_spec[%d]: source_id %d not found", i, pf.SourceID)
		}
		partitionSpec[i] = CompiledPartitionField{
			SourceID:    pf.SourceID,
			FieldID:     pf.FieldID,
			Transform:   pf.Transform,
			Param:       pf.Param,
			Name:        pf.Name,
			ColumnIndex: colIndex,
		}
	}

	// sort order - map source_id to column index
	sortOrder := make([]CompiledSortField, len(config.SortOrder))
	for i, sf := range config.SortOrder {
		colIndex, ok := fieldIDToIndex[sf.SourceID]
		if !ok {
			return nil, fmt.Errorf("sort_order[%d]: source_id %d not found", i, sf.SourceID)
		}
		sortOrder[i] = CompiledSortField{
			SourceID:    sf.SourceID,
			Transform:   sf.Transform,
			Direction:   sf.Direction,
			NullOrder:   sf.NullOrder,
			ColumnIndex: colIndex,
		}
	}

	// Arrow output schema with schema ID for iceberg.schema metadata
	outputSchema, err := BuildArrowSchemaWithMetadata(columns, partitionSpec, sortOrder, config.SchemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to build output schema: %w", err)
	}

	return &CompiledPipeline{
		Config:        config,
		SchemaID:      config.SchemaID,
		SchemaName:    schemaName,
		Validations:   validations,
		Filter:        filter,
		Columns:       columns,
		OutputSchema:  outputSchema,
		PartitionSpec: partitionSpec,
		SortOrder:     sortOrder,
	}, nil
}

// compileExpression compiles a CEL expression and verifies its type.
func compileExpression(env *cel.Env, expr string, expectedType *cel.Type) (cel.Program, error) {
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	if ast == nil {
		return nil, fmt.Errorf("compilation produced nil AST")
	}

	// Type check if expected type is specified
	// Check if output type is assignable TO expected type
	if expectedType != nil {
		outputType := ast.OutputType()
		if !expectedType.IsAssignableType(outputType) {
			return nil, fmt.Errorf("type mismatch: expected %s, got %s", expectedType, outputType)
		}
	}

	prog, err := env.Program(ast)
	if err != nil {
		return nil, err
	}

	return prog, nil
}

// getAdapter creates the appropriate adapter based on the mapped schema type.
func getAdapter(mapped *celschema.MappedSchema, format SchemaFormat) (celschema.SchemaAdapter, error) {
	if mapped == nil {
		return nil, fmt.Errorf("nil mapped schema")
	}

	switch format {
	case SchemaFormatJSON:
		return celschema.NewJsonSchemaAdapter(mapped)
	case SchemaFormatAvro:
		return celschema.NewAvroAdapter(mapped)
	}

	// Auto-detect no harm
	jsonAdapter, err := celschema.NewJsonSchemaAdapter(mapped)
	if err == nil {
		return jsonAdapter, nil
	}

	avroAdapter, err := celschema.NewAvroAdapter(mapped)
	if err == nil {
		return avroAdapter, nil
	}

	return nil, fmt.Errorf("failed to create adapter for schema type: %T (specify schema_format in x-pipeline)", mapped.Raw)
}
