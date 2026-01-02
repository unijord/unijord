package schemaregistry

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hamba/avro/v2"
	"github.com/santhosh-tekuri/jsonschema/v5"
	bolt "go.etcd.io/bbolt"

	"github.com/unijord/unijord/pkg/pipeline"
)

// SchemaType represents the type of schema.
type SchemaType uint8

const (
	SchemaTypeAVRO SchemaType = iota + 1
	SchemaTypeJSON
)

func (t SchemaType) String() string {
	switch t {
	case SchemaTypeAVRO:
		return "AVRO"
	case SchemaTypeJSON:
		return "JSON"
	default:
		return "UNKNOWN"
	}
}

var (
	ErrSchemaNotFound      = errors.New("schema not found")
	ErrSchemaExists        = errors.New("schema already exists")
	ErrInvalidSchema       = errors.New("invalid schema")
	ErrUnsupportedType     = errors.New("unsupported schema type")
	ErrDecodingFailed      = errors.New("decoding failed")
	ErrPipelineCompilation = errors.New("pipeline compilation failed")
)

var (
	// schema_id -> schema entry (type + JSON)
	bucketSchemas = []byte("schemas")
)

// Validator provides schema decoding capabilities.
type Validator interface {
	// Decode decodes binary data to a native Go value.
	// For Avro: validates structure during unmarshal.
	// For JSON: validates against JSON Schema before unmarshaling.
	Decode(data []byte, target interface{}) error
}

// Schema holds the schema definition, validator, and compiled pipeline.
type Schema struct {
	ID   uint32
	Type SchemaType
	// without x-pipeline
	SchemaJSON    string
	RawSchemaJSON string
	Validator     Validator

	// Pipeline fields if has x-pipeline
	PipelineConfig   *pipeline.Config
	CompiledPipeline *pipeline.CompiledPipeline
	PipelineExecutor *pipeline.Executor
	OutputSchema     *arrow.Schema
}

// HasPipeline returns true if the schema has an embedded pipeline.
func (s *Schema) HasPipeline() bool {
	return s.CompiledPipeline != nil
}

// avroValidator implements Validator for AVRO schemas.
type avroValidator struct {
	schema avro.Schema
}

func newAVROValidator(schemaJSON string) (*avroValidator, error) {
	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSchema, err)
	}
	return &avroValidator{schema: schema}, nil
}

func (v *avroValidator) Decode(data []byte, target interface{}) error {
	if err := avro.Unmarshal(v.schema, data, target); err != nil {
		return fmt.Errorf("%w: %v", ErrDecodingFailed, err)
	}
	return nil
}

// AvroSchema returns the underlying avro.Schema.
func (v *avroValidator) AvroSchema() avro.Schema {
	return v.schema
}

// jsonValidator implements Validator for JSON schemas.
type jsonValidator struct {
	schema *jsonschema.Schema
}

func newJSONValidator(schemaJSON string) (*jsonValidator, error) {
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", strings.NewReader(schemaJSON)); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON schema: %v", ErrInvalidSchema, err)
	}

	schema, err := compiler.Compile("schema.json")
	if err != nil {
		return nil, fmt.Errorf("%w: failed to compile JSON schema: %v", ErrInvalidSchema, err)
	}

	return &jsonValidator{schema: schema}, nil
}

func (v *jsonValidator) Decode(data []byte, target interface{}) error {
	var value interface{}
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("%w: invalid JSON: %v", ErrDecodingFailed, err)
	}

	if err := v.schema.Validate(value); err != nil {
		return fmt.Errorf("%w: %v", ErrDecodingFailed, err)
	}

	if t, ok := target.(*map[string]any); ok {
		if m, ok := value.(map[string]interface{}); ok {
			*t = m
			return nil
		}
	}

	// for other target types use json.Unmarshal.
	return json.Unmarshal(data, target)
}

// schemaEntry is used for persistence.
type schemaEntry struct {
	Type SchemaType `json:"type"`
	// x-pipeline removed
	SchemaJSON    string `json:"schema"`
	RawSchemaJSON string `json:"raw_schema"`
}

// Registry stores and retrieves schemas by ID.
type Registry struct {
	mu     sync.RWMutex
	cache  map[uint32]*Schema
	db     *bolt.DB
	dbPath string
}

// NewRegistry creates a new schema registry backed by BoltDB.
func NewRegistry(dbPath string) (*Registry, error) {
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open schema db: %w", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketSchemas)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("init bucket: %w", err)
	}

	return &Registry{
		cache:  make(map[uint32]*Schema),
		db:     db,
		dbPath: dbPath,
	}, nil
}

// Register stores a schema with the given ID.
func (r *Registry) Register(id uint32, schemaType SchemaType, schemaJSON string) error {
	cleanSchema, pipelineConfig, err := pipeline.ExtractSchemaWithPipeline([]byte(schemaJSON))
	if err != nil {
		return fmt.Errorf("extract pipeline: %w", err)
	}

	validator, err := createValidator(schemaType, string(cleanSchema))
	if err != nil {
		return err
	}

	normalizedClean, err := normalizeSchema(string(cleanSchema))
	if err != nil {
		return fmt.Errorf("normalize clean schema: %w", err)
	}
	normalizedRaw, err := normalizeSchema(schemaJSON)
	if err != nil {
		return fmt.Errorf("normalize raw schema: %w", err)
	}

	var compiled *pipeline.CompiledPipeline
	var executor *pipeline.Executor
	var outputSchema *arrow.Schema

	if pipelineConfig != nil {
		compiled, err = compilePipeline(schemaType, cleanSchema, pipelineConfig)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrPipelineCompilation, err)
		}
		executor = pipeline.NewExecutor(compiled)
		outputSchema = compiled.OutputSchema
	}

	entry := schemaEntry{
		Type:          schemaType,
		SchemaJSON:    normalizedClean,
		RawSchemaJSON: normalizedRaw,
	}
	entryBytes, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}

	err = r.db.Update(func(tx *bolt.Tx) error {
		schemas := tx.Bucket(bucketSchemas)

		var key [4]byte
		binary.BigEndian.PutUint32(key[:], id)

		if schemas.Get(key[:]) != nil {
			return ErrSchemaExists
		}

		return schemas.Put(key[:], entryBytes)
	})
	if err != nil {
		return fmt.Errorf("store schema: %w", err)
	}

	r.mu.Lock()
	r.cache[id] = &Schema{
		ID:               id,
		Type:             schemaType,
		SchemaJSON:       normalizedClean,
		RawSchemaJSON:    normalizedRaw,
		Validator:        validator,
		PipelineConfig:   pipelineConfig,
		CompiledPipeline: compiled,
		PipelineExecutor: executor,
		OutputSchema:     outputSchema,
	}
	r.mu.Unlock()

	return nil
}

// Get retrieves a schema by ID.
func (r *Registry) Get(id uint32) (*Schema, error) {
	r.mu.RLock()
	if schema, ok := r.cache[id]; ok {
		r.mu.RUnlock()
		return schema, nil
	}
	r.mu.RUnlock()

	// slow path
	r.mu.Lock()
	defer r.mu.Unlock()
	if schema, ok := r.cache[id]; ok {
		return schema, nil
	}
	schema, err := r.loadAndCompile(id)
	if err != nil {
		return nil, err
	}

	r.cache[id] = schema
	return schema, nil
}

func (r *Registry) loadAndCompile(id uint32) (*Schema, error) {
	var entryBytes []byte
	err := r.db.View(func(tx *bolt.Tx) error {
		schemas := tx.Bucket(bucketSchemas)
		var key [4]byte
		binary.BigEndian.PutUint32(key[:], id)
		entryBytes = schemas.Get(key[:])
		if entryBytes == nil {
			return ErrSchemaNotFound
		}
		entryBytes = append([]byte(nil), entryBytes...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	var entry schemaEntry
	if err := json.Unmarshal(entryBytes, &entry); err != nil {
		return nil, fmt.Errorf("unmarshal entry: %w", err)
	}

	validator, err := createValidator(entry.Type, entry.SchemaJSON)
	if err != nil {
		return nil, fmt.Errorf("create validator for schema %d: %w", id, err)
	}

	var pipelineConfig *pipeline.Config
	var compiled *pipeline.CompiledPipeline
	var executor *pipeline.Executor
	var outputSchema *arrow.Schema

	if entry.RawSchemaJSON != "" {
		_, pipelineConfig, err = pipeline.ExtractSchemaWithPipeline([]byte(entry.RawSchemaJSON))
		if err != nil {
			return nil, fmt.Errorf("extract pipeline for schema %d: %w", id, err)
		}

		if pipelineConfig != nil {
			compiled, err = compilePipeline(entry.Type, []byte(entry.SchemaJSON), pipelineConfig)
			if err != nil {
				return nil, fmt.Errorf("compile pipeline for schema %d: %w", id, err)
			}
			executor = pipeline.NewExecutor(compiled)
			outputSchema = compiled.OutputSchema
		}
	}

	return &Schema{
		ID:               id,
		Type:             entry.Type,
		SchemaJSON:       entry.SchemaJSON,
		RawSchemaJSON:    entry.RawSchemaJSON,
		Validator:        validator,
		PipelineConfig:   pipelineConfig,
		CompiledPipeline: compiled,
		PipelineExecutor: executor,
		OutputSchema:     outputSchema,
	}, nil
}

// GetValidator is a convenience method to get the validator directly.
func (r *Registry) GetValidator(id uint32) (Validator, error) {
	schema, err := r.Get(id)
	if err != nil {
		return nil, err
	}
	return schema.Validator, nil
}

// HasPipeline checks if a schema has an embedded pipeline.
func (r *Registry) HasPipeline(id uint32) bool {
	schema, err := r.Get(id)
	if err != nil {
		return false
	}
	return schema.HasPipeline()
}

// Close closes the registry database.
func (r *Registry) Close() error {
	return r.db.Close()
}

func createValidator(schemaType SchemaType, schemaJSON string) (Validator, error) {
	switch schemaType {
	case SchemaTypeAVRO:
		return newAVROValidator(schemaJSON)
	case SchemaTypeJSON:
		return newJSONValidator(schemaJSON)
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedType, schemaType)
	}
}

func normalizeSchema(schemaJSON string) (string, error) {
	var schema interface{}
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return "", err
	}
	normalized, err := json.Marshal(schema)
	if err != nil {
		return "", err
	}
	return string(normalized), nil
}
