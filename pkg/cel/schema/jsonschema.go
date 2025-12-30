package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// TODO: IMP TO Communicate.
// Currently JSON Schema Support is minimal and not all the property is supported.

var (
	// ErrNotObjectSchema is returned when root schema is not an object type.
	ErrNotObjectSchema = errors.New("root schema must be an object type")

	// ErrInvalidJSONSchema is returned when schema JSON is invalid.
	ErrInvalidJSONSchema = errors.New("invalid JSON schema")
)

// jsonSchemaNode represents a parsed JSON Schema node.
type jsonSchemaNode struct {
	// https://json-schema.org/learn/getting-started-step-by-step
	// {
	//  "$schema": "https://json-schema.org/draft/2020-12/schema",
	//  "$id": "https://example.com/product.schema.json",
	//  "title": "Product",
	//  "description": "A product in the catalog",
	//  "type": "object"
	//}
	ID    string         `json:"$id"`
	Title string         `json:"title"`
	Type  jsonSchemaType `json:"type"`
	// https://www.learnjsonschema.com/2020-12/core/ref/
	Ref string `json:"$ref"`

	// https://www.learnjsonschema.com/2020-12/core/defs/
	Defs        map[string]*jsonSchemaNode `json:"$defs"`
	Definitions map[string]*jsonSchemaNode `json:"definitions"`

	Properties           map[string]*jsonSchemaNode `json:"properties"`
	AdditionalProperties *jsonSchemaNode            `json:"additionalProperties"`
	// prefixItems ?, currently not supported. Think if we need it before adding.
	Items *jsonSchemaNode `json:"items"`

	AnyOf []*jsonSchemaNode `json:"anyOf"`
	OneOf []*jsonSchemaNode `json:"oneOf"`
	AllOf []*jsonSchemaNode `json:"allOf"`

	// semantic hint for string values
	// https://www.learnjsonschema.com/2020-12/format-annotation/format/
	// list of format.
	Format string `json:"format"`

	Enum  []any `json:"enum"`
	Const any   `json:"const"`
}

// jsonSchemaType handles both string and array type declarations as
// the type field can be either a single string or an array of strings
// in the JSON Schema.
type jsonSchemaType []string

func (t *jsonSchemaType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*t = []string{s}
		return nil
	}

	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		*t = arr
		return nil
	}

	return nil
}

type JsonSchemaMapper struct {
	SkipValidation bool
}

// NewJsonSchemaMapper creates a new JsonSchemaMapper with validation enabled.
func NewJsonSchemaMapper() *JsonSchemaMapper {
	return &JsonSchemaMapper{}
}

// Map parses JSON Schema and returns a MappedSchema.
func (m *JsonSchemaMapper) Map(schema []byte) (*MappedSchema, error) {
	if !m.SkipValidation {
		if err := m.validate(schema); err != nil {
			return nil, err
		}
	}

	// Step 2: Parse for CEL type building (using simple parser)
	var node jsonSchemaNode
	if err := json.Unmarshal(schema, &node); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidJSONSchema, err)
	}

	// Root must be an object with properties
	if !node.isObject() {
		return nil, ErrNotObjectSchema
	}

	return &MappedSchema{Raw: &node}, nil
}

// validate checks if the schema is a valid JSON Schema.
func (m *JsonSchemaMapper) validate(schema []byte) error {
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(schema)))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidJSONSchema, err)
	}

	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", doc); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidJSONSchema, err)
	}

	_, err = compiler.Compile("schema.json")
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidJSONSchema, err)
	}

	return nil
}

func (n *jsonSchemaNode) isObject() bool {
	for _, t := range n.Type {
		if t == "object" {
			return true
		}
	}
	return len(n.Properties) > 0
}

func (n *jsonSchemaNode) isArray() bool {
	for _, t := range n.Type {
		if t == "array" {
			return true
		}
	}
	return false
}

func (n *jsonSchemaNode) hasType(typeName string) bool {
	for _, t := range n.Type {
		if t == typeName {
			return true
		}
	}
	return false
}

// CEL needs unique type names. If the nested object has $id or title
// it returns it.
func (n *jsonSchemaNode) getName() string {
	if n.ID != "" {
		// Strip URL parts if present
		name := n.ID
		if idx := strings.LastIndex(name, "/"); idx >= 0 {
			name = name[idx+1:]
		}
		if idx := strings.LastIndex(name, "#"); idx >= 0 {
			name = name[:idx]
		}
		return name
	}
	if n.Title != "" {
		return strings.ReplaceAll(n.Title, " ", "")
	}
	return ""
}

// JsonSchemaAdapter implements SchemaAdapter for JSON Schema.
type JsonSchemaAdapter struct {
	root        *jsonSchemaNode
	definitions map[string]*jsonSchemaNode
	typeCounter int
}

// NewJsonSchemaAdapter creates an adapter from a MappedSchema containing a JSON Schema.
func NewJsonSchemaAdapter(mapped *MappedSchema) (*JsonSchemaAdapter, error) {
	if mapped == nil {
		return nil, ErrNilSchema
	}

	node, ok := mapped.Raw.(*jsonSchemaNode)
	if !ok {
		return nil, fmt.Errorf("expected *jsonSchemaNode, got %T", mapped.Raw)
	}

	// Merge definitions and $defs
	defs := make(map[string]*jsonSchemaNode)
	for k, v := range node.Definitions {
		defs[k] = v
	}
	for k, v := range node.Defs {
		defs[k] = v
	}

	return &JsonSchemaAdapter{
		root:        node,
		definitions: defs,
	}, nil
}

// BuildTypes implements SchemaAdapter.
func (a *JsonSchemaAdapter) BuildTypes(provider *TypeProvider) (string, error) {
	if a.root == nil {
		return "", ErrNilSchema
	}

	rootName := a.root.getName()
	if rootName == "" {
		rootName = "Root"
	}

	a.buildProvider(a.root, rootName, provider)
	return rootName, nil
}

func (a *JsonSchemaAdapter) buildProvider(node *jsonSchemaNode, typeName string, p *TypeProvider) {
	if node == nil || !node.isObject() {
		return
	}

	fields := make(map[string]*cel.Type)

	// Sort keys for deterministic output
	keys := make([]string, 0, len(node.Properties))
	for k := range node.Properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, fieldName := range keys {
		prop := node.Properties[fieldName]
		fields[fieldName] = a.mapTypeToCel(prop, typeName+"_"+fieldName, p)
	}

	p.registerType(typeName, fields)
}

func (a *JsonSchemaAdapter) mapTypeToCel(node *jsonSchemaNode, pathName string, p *TypeProvider) *cel.Type {
	if node == nil {
		return cel.DynType
	}

	// Handle $ref
	if node.Ref != "" {
		resolved := a.resolveRef(node.Ref)
		if resolved != nil {
			return a.mapTypeToCel(resolved, pathName, p)
		}
		return cel.DynType
	}

	// Handle allOf (schema composition)
	if len(node.AllOf) > 0 {
		return a.mapAllOfToCel(node.AllOf, pathName, p)
	}

	// Handle anyOf/oneOf (union types)
	if len(node.AnyOf) > 0 {
		return a.mapUnionToCel(node.AnyOf, pathName, p)
	}
	if len(node.OneOf) > 0 {
		return a.mapUnionToCel(node.OneOf, pathName, p)
	}

	// Handle enum (infer type from values)
	if len(node.Enum) > 0 {
		return a.mapEnumToCel(node.Enum)
	}

	// Handle const (infer type from value)
	if node.Const != nil {
		return a.mapConstToCel(node.Const)
	}

	// Handle nullable types like ["string", "null"]
	if len(node.Type) > 1 {
		return a.mapMultiTypeToCel(node, pathName, p)
	}

	// Single type
	if len(node.Type) == 1 {
		return a.mapSingleTypeToCel(node, node.Type[0], pathName, p)
	}

	// Infer type from structure
	if node.isObject() {
		return a.mapObjectToCel(node, pathName, p)
	}
	if node.isArray() {
		return a.mapArrayToCel(node, pathName, p)
	}

	return cel.DynType
}

func (a *JsonSchemaAdapter) mapSingleTypeToCel(node *jsonSchemaNode, typeName string, pathName string, p *TypeProvider) *cel.Type {
	switch typeName {
	case "string":
		return a.mapStringToCel(node)
	case "integer":
		return cel.IntType
	case "number":
		return cel.DoubleType
	case "boolean":
		return cel.BoolType
	case "null":
		return cel.NullType
	case "array":
		return a.mapArrayToCel(node, pathName, p)
	case "object":
		return a.mapObjectToCel(node, pathName, p)
	default:
		return cel.DynType
	}
}

// https://www.learnjsonschema.com/2020-12/format-annotation/format/
func (a *JsonSchemaAdapter) mapStringToCel(node *jsonSchemaNode) *cel.Type {
	switch node.Format {
	case "date-time", "date":
		return cel.TimestampType
	case "time", "duration":
		return cel.DurationType
	case "uuid", "email", "uri", "hostname", "ipv4", "ipv6":
		return cel.StringType
	// OpenAPI 3.0 formats (not JSON Schema standard)
	// See: https://spec.openapis.org/registry/format/
	// Embedding small binary data in JSON ? typically for some use cases.
	case "byte", "binary":
		return cel.BytesType
	default:
		return cel.StringType
	}
}

func (a *JsonSchemaAdapter) mapArrayToCel(node *jsonSchemaNode, pathName string, p *TypeProvider) *cel.Type {
	if node.Items == nil {
		return cel.ListType(cel.DynType)
	}
	elemType := a.mapTypeToCel(node.Items, pathName+"_item", p)
	return cel.ListType(elemType)
}

func (a *JsonSchemaAdapter) mapObjectToCel(node *jsonSchemaNode, pathName string, p *TypeProvider) *cel.Type {
	// Object with properties -> named struct type
	if len(node.Properties) > 0 {
		nestedName := node.getName()
		// fallback to path: "Order_customer_address" like this
		if nestedName == "" {
			nestedName = pathName
		}
		a.buildProvider(node, nestedName, p)
		return cel.ObjectType(nestedName)
	}

	// Object with additionalProperties -> map type
	if node.AdditionalProperties != nil {
		valType := a.mapTypeToCel(node.AdditionalProperties, pathName+"_value", p)
		return cel.MapType(cel.StringType, valType)
	}

	// Generic object -> map<string, dyn>
	return cel.MapType(cel.StringType, cel.DynType)
}

func (a *JsonSchemaAdapter) mapMultiTypeToCel(node *jsonSchemaNode, pathName string, p *TypeProvider) *cel.Type {
	// Filter out null to find the actual type
	var nonNullTypes []string
	for _, t := range node.Type {
		if t != "null" {
			nonNullTypes = append(nonNullTypes, t)
		}
	}

	// Single non-null type (nullable)
	if len(nonNullTypes) == 1 {
		return a.mapSingleTypeToCel(node, nonNullTypes[0], pathName, p)
	}

	// Multiple non-null types -> dynamic
	return cel.DynType
}

func (a *JsonSchemaAdapter) mapUnionToCel(schemas []*jsonSchemaNode, pathName string, p *TypeProvider) *cel.Type {
	var nonNullType *cel.Type

	for _, s := range schemas {
		if s.hasType("null") {
			continue
		}

		mapped := a.mapTypeToCel(s, pathName, p)
		if nonNullType == nil {
			nonNullType = mapped
		} else {
			// Multiple non-null types -> dynamic
			return cel.DynType
		}
	}

	if nonNullType == nil {
		return cel.NullType
	}
	return nonNullType
}

func (a *JsonSchemaAdapter) resolveRef(ref string) *jsonSchemaNode {
	// Handle #/definitions/Name or #/$defs/Name
	if strings.HasPrefix(ref, "#/definitions/") {
		name := strings.TrimPrefix(ref, "#/definitions/")
		return a.definitions[name]
	}
	if strings.HasPrefix(ref, "#/$defs/") {
		name := strings.TrimPrefix(ref, "#/$defs/")
		return a.definitions[name]
	}
	return nil
}

// mapAllOfToCel merges properties from all schemas in allOf.
func (a *JsonSchemaAdapter) mapAllOfToCel(schemas []*jsonSchemaNode, pathName string, p *TypeProvider) *cel.Type {
	// Merge all properties into a single object
	merged := &jsonSchemaNode{
		Properties: make(map[string]*jsonSchemaNode),
	}

	for _, s := range schemas {
		// Resolve $ref if present
		resolved := s
		if s.Ref != "" {
			resolved = a.resolveRef(s.Ref)
			if resolved == nil {
				continue
			}
		}

		// Merge properties
		for k, v := range resolved.Properties {
			merged.Properties[k] = v
		}

		// Take title/id from first schema that has it
		if merged.Title == "" && resolved.Title != "" {
			merged.Title = resolved.Title
		}
		if merged.ID == "" && resolved.ID != "" {
			merged.ID = resolved.ID
		}
	}

	if len(merged.Properties) > 0 {
		return a.mapObjectToCel(merged, pathName, p)
	}

	return cel.DynType
}

// Check THIS.
// JSON unmarshals all numbers as float64
// Check if it's actually an integer

// mapEnumToCel infers CEL type from enum values.
func (a *JsonSchemaAdapter) mapEnumToCel(values []any) *cel.Type {
	if len(values) == 0 {
		return cel.DynType
	}
	switch v := values[0].(type) {
	case string:
		return cel.StringType
	case float64:
		if v == float64(int64(v)) {
			return cel.IntType
		}
		return cel.DoubleType
	case float32:
		if v == float32(int32(v)) {
			return cel.IntType
		}
		return cel.DoubleType
	case int, int64, int32:
		return cel.IntType
	case bool:
		return cel.BoolType
	default:
		// Default to string for enums
		return cel.StringType
	}
}

// mapConstToCel infers CEL type from const value.
func (a *JsonSchemaAdapter) mapConstToCel(value any) *cel.Type {
	switch v := value.(type) {
	case string:
		return cel.StringType
	case float64:
		if v == float64(int64(v)) {
			return cel.IntType
		}
		return cel.DoubleType
	case float32:
		if v == float32(int32(v)) {
			return cel.IntType
		}
		return cel.DoubleType
	case int, int64, int32:
		return cel.IntType
	case bool:
		return cel.BoolType
	case nil:
		return cel.NullType
	default:
		return cel.DynType
	}
}
