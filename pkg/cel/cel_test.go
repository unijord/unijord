package cel

import (
	"errors"
	"testing"

	"github.com/google/cel-go/cel"
)

func TestExprType_String(t *testing.T) {
	tests := []struct {
		typ  ExprType
		want string
	}{
		{ExprTypeBool, "bool"},
		{ExprTypeInt, "int"},
		{ExprTypeDouble, "double"},
		{ExprTypeString, "string"},
		{ExprTypeBytes, "bytes"},
		{ExprTypeList, "list"},
		{ExprTypeMap, "map"},
		{ExprTypeDyn, "dyn"},
		{ExprTypeUnknown, "unknown"},
	}
	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("ExprType(%d).String() = %s, want %s", tt.typ, got, tt.want)
		}
	}
}

func TestEvalResult_Ok(t *testing.T) {
	r := NewEvalResult("hello", ExprTypeString)
	if !r.Ok() {
		t.Error("expected Ok() = true")
	}
	if r.Err() != nil {
		t.Error("expected Err() = nil")
	}
}

func TestEvalResult_Error(t *testing.T) {
	r := NewEvalError(errTest)
	if r.Ok() {
		t.Error("expected Ok() = false")
	}
	if !errors.Is(r.Err(), errTest) {
		t.Errorf("expected Err() = errTest, got %v", r.Err())
	}
}

var errTest = &testError{}

type testError struct{}

func (e *testError) Error() string { return "test error" }

func TestEvalResult_Bool(t *testing.T) {
	r := NewEvalResult(true, ExprTypeBool)
	v, err := r.Bool()
	if err != nil {
		t.Fatalf("Bool() error: %v", err)
	}
	if !v {
		t.Error("expected true")
	}

	r2 := NewEvalResult("not bool", ExprTypeString)
	_, err = r2.Bool()
	if err == nil {
		t.Error("expected error for non-bool")
	}
}

func TestEvalResult_Int(t *testing.T) {
	r := NewEvalResult(int64(42), ExprTypeInt)
	v, err := r.Int()
	if err != nil {
		t.Fatalf("Int() error: %v", err)
	}
	if v != 42 {
		t.Errorf("expected 42, got %d", v)
	}
}

func TestEvalResult_String(t *testing.T) {
	r := NewEvalResult("hello", ExprTypeString)
	v, err := r.String()
	if err != nil {
		t.Fatalf("String() error: %v", err)
	}
	if v != "hello" {
		t.Errorf("expected hello, got %s", v)
	}
}

func TestEnvBuilder_Build(t *testing.T) {
	env, err := NewEnvBuilder().
		WithVariable("x", cel.IntType).
		WithVariable("name", cel.StringType).
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if env == nil {
		t.Fatal("expected non-nil env")
	}
}

func TestEnvBuilder_WithEnvelope(t *testing.T) {
	env, err := NewEnvBuilder().
		WithEnvelope().
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	// Should be able to compile expression using envelope fields
	_, issues := env.Compile("_occurred_at > 0")
	if issues != nil && issues.Err() != nil {
		t.Errorf("compile with envelope field: %v", issues.Err())
	}
}

func TestCompiler_Compile(t *testing.T) {
	env, _ := NewEnvBuilder().
		WithVariable("x", cel.IntType).
		Build()

	c := NewCompiler(env)
	expr, err := c.Compile("x > 10")
	if err != nil {
		t.Fatalf("Compile() error: %v", err)
	}
	if expr.Source() != "x > 10" {
		t.Errorf("Source() = %s, want x > 10", expr.Source())
	}
	if expr.OutputType() != ExprTypeBool {
		t.Errorf("OutputType() = %s, want bool", expr.OutputType())
	}
}

func TestCompiler_CompileError(t *testing.T) {
	env, _ := NewEnvBuilder().Build()
	c := NewCompiler(env)

	_, err := c.Compile("invalid !!!")
	if err == nil {
		t.Error("expected compile error")
	}
}

func TestCompiler_CompileBool(t *testing.T) {
	env, _ := NewEnvBuilder().
		WithVariable("x", cel.IntType).
		Build()
	c := NewCompiler(env)

	// Valid bool expression
	_, err := c.CompileBool("x > 10")
	if err != nil {
		t.Fatalf("CompileBool() error: %v", err)
	}

	// Non-bool expression
	_, err = c.CompileBool("x + 10")
	if err == nil {
		t.Error("expected error for non-bool expression")
	}
}

func TestCompiler_CompileString(t *testing.T) {
	env, _ := NewEnvBuilder().
		WithVariable("name", cel.StringType).
		Build()
	c := NewCompiler(env)

	expr, err := c.CompileString("name")
	if err != nil {
		t.Fatalf("CompileString() error: %v", err)
	}
	if expr.OutputType() != ExprTypeString {
		t.Errorf("OutputType() = %s, want string", expr.OutputType())
	}
}

func TestEvaluator_Eval(t *testing.T) {
	env, _ := NewEnvBuilder().
		WithVariable("x", cel.IntType).
		WithVariable("y", cel.IntType).
		Build()
	c := NewCompiler(env)
	e := NewEvaluator()

	expr, _ := c.Compile("x + y")
	result := e.Eval(expr, map[string]any{"x": int64(10), "y": int64(20)})

	if !result.Ok() {
		t.Fatalf("Eval() error: %v", result.Err())
	}
	v, _ := result.Int()
	if v != 30 {
		t.Errorf("expected 30, got %d", v)
	}
}

func TestEvaluator_EvalBool(t *testing.T) {
	env, _ := NewEnvBuilder().
		WithVariable("amount", cel.DoubleType).
		Build()
	c := NewCompiler(env)
	e := NewEvaluator()

	expr, _ := c.CompileBool("amount > 100.0")

	tests := []struct {
		amount float64
		want   bool
	}{
		{50.0, false},
		{100.0, false},
		{150.0, true},
	}

	for _, tt := range tests {
		got, err := e.EvalBool(expr, map[string]any{"amount": tt.amount})
		if err != nil {
			t.Errorf("EvalBool(amount=%f) error: %v", tt.amount, err)
			continue
		}
		if got != tt.want {
			t.Errorf("EvalBool(amount=%f) = %v, want %v", tt.amount, got, tt.want)
		}
	}
}

func TestEvaluator_EvalString(t *testing.T) {
	env, _ := NewEnvBuilder().
		WithVariable("first", cel.StringType).
		WithVariable("last", cel.StringType).
		Build()
	c := NewCompiler(env)
	e := NewEvaluator()

	expr, _ := c.CompileString("first + ' ' + last")
	got, err := e.EvalString(expr, map[string]any{"first": "John", "last": "Doe"})
	if err != nil {
		t.Fatalf("EvalString() error: %v", err)
	}
	if got != "John Doe" {
		t.Errorf("got %s, want John Doe", got)
	}
}

func TestEvaluator_Ternary(t *testing.T) {
	env, _ := NewEnvBuilder().
		WithVariable("age", cel.IntType).
		Build()
	c := NewCompiler(env)
	e := NewEvaluator()

	expr, _ := c.CompileString("age < 18 ? 'minor' : 'adult'")

	tests := []struct {
		age  int64
		want string
	}{
		{10, "minor"},
		{18, "adult"},
		{30, "adult"},
	}

	for _, tt := range tests {
		got, err := e.EvalString(expr, map[string]any{"age": tt.age})
		if err != nil {
			t.Errorf("age=%d error: %v", tt.age, err)
			continue
		}
		if got != tt.want {
			t.Errorf("age=%d got %s, want %s", tt.age, got, tt.want)
		}
	}
}

func TestActivationPool(t *testing.T) {
	pool := newActivationPool()

	vars := map[string]any{"x": int64(10), "y": "hello"}
	a := pool.get(vars)

	v, ok := a.ResolveName("x")
	if !ok || v != int64(10) {
		t.Errorf("ResolveName(x) = %v, %v", v, ok)
	}

	v, ok = a.ResolveName("y")
	if !ok || v != "hello" {
		t.Errorf("ResolveName(y) = %v, %v", v, ok)
	}

	_, ok = a.ResolveName("z")
	if ok {
		t.Error("expected z not found")
	}

	if a.Parent() != nil {
		t.Error("expected nil parent")
	}

	pool.put(a)

	// Get again - should reuse
	a2 := pool.get(map[string]any{"z": true})
	_, ok = a2.ResolveName("x")
	if ok {
		t.Error("expected x cleared after put")
	}
	v, ok = a2.ResolveName("z")
	if !ok || v != true {
		t.Errorf("ResolveName(z) = %v, %v", v, ok)
	}
	pool.put(a2)
}
