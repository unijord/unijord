package cel

import (
	"fmt"

	"github.com/google/cel-go/cel"
)

type ExprType int

const (
	ExprTypeUnknown ExprType = iota
	ExprTypeBool
	ExprTypeInt
	ExprTypeDouble
	ExprTypeString
	ExprTypeBytes
	ExprTypeTimestamp
	ExprTypeDuration
	ExprTypeList
	ExprTypeMap
	ExprTypeDyn
	ExprTypeNull
)

func (t ExprType) String() string {
	switch t {
	case ExprTypeBool:
		return "bool"
	case ExprTypeInt:
		return "int"
	case ExprTypeDouble:
		return "double"
	case ExprTypeString:
		return "string"
	case ExprTypeBytes:
		return "bytes"
	case ExprTypeTimestamp:
		return "timestamp"
	case ExprTypeDuration:
		return "duration"
	case ExprTypeList:
		return "list"
	case ExprTypeMap:
		return "map"
	case ExprTypeDyn:
		return "dyn"
	case ExprTypeNull:
		return "null"
	default:
		return "unknown"
	}
}

type CompiledExpr struct {
	source     string
	program    cel.Program
	outputType ExprType
}

func (c *CompiledExpr) Source() string     { return c.source }
func (c *CompiledExpr) OutputType() ExprType { return c.outputType }
func (c *CompiledExpr) Program() cel.Program { return c.program }

type EvalResult struct {
	value any
	typ   ExprType
	err   error
}

func NewEvalResult(value any, typ ExprType) EvalResult {
	return EvalResult{value: value, typ: typ}
}

func NewEvalError(err error) EvalResult {
	return EvalResult{err: err}
}

func (r EvalResult) Value() any      { return r.value }
func (r EvalResult) Type() ExprType  { return r.typ }
func (r EvalResult) Err() error      { return r.err }
func (r EvalResult) Ok() bool        { return r.err == nil }

func (r EvalResult) Bool() (bool, error) {
	if r.err != nil {
		return false, r.err
	}
	if b, ok := r.value.(bool); ok {
		return b, nil
	}
	return false, fmt.Errorf("expected bool, got %T", r.value)
}

func (r EvalResult) Int() (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	if i, ok := r.value.(int64); ok {
		return i, nil
	}
	return 0, fmt.Errorf("expected int64, got %T", r.value)
}

func (r EvalResult) Double() (float64, error) {
	if r.err != nil {
		return 0, r.err
	}
	if f, ok := r.value.(float64); ok {
		return f, nil
	}
	return 0, fmt.Errorf("expected float64, got %T", r.value)
}

func (r EvalResult) String() (string, error) {
	if r.err != nil {
		return "", r.err
	}
	if s, ok := r.value.(string); ok {
		return s, nil
	}
	return "", fmt.Errorf("expected string, got %T", r.value)
}

func (r EvalResult) Bytes() ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	if b, ok := r.value.([]byte); ok {
		return b, nil
	}
	return nil, fmt.Errorf("expected []byte, got %T", r.value)
}

func exprTypeFromCEL(t *cel.Type) ExprType {
	if t == nil {
		return ExprTypeUnknown
	}
	switch t {
	case cel.BoolType:
		return ExprTypeBool
	case cel.IntType:
		return ExprTypeInt
	case cel.DoubleType:
		return ExprTypeDouble
	case cel.StringType:
		return ExprTypeString
	case cel.BytesType:
		return ExprTypeBytes
	case cel.TimestampType:
		return ExprTypeTimestamp
	case cel.DurationType:
		return ExprTypeDuration
	case cel.NullType:
		return ExprTypeNull
	case cel.DynType:
		return ExprTypeDyn
	default:
		switch t.Kind() {
		case cel.ListKind:
			return ExprTypeList
		case cel.MapKind:
			return ExprTypeMap
		default:
			return ExprTypeDyn
		}
	}
}
