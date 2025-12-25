package schema_test

import (
	"testing"

	"github.com/unijord/unijord/pkg/cel"
	"github.com/unijord/unijord/pkg/cel/schema"
)

var orderSchema = `{
	"type": "record",
	"name": "Order",
	"fields": [
		{"name": "order_id", "type": "string"},
		{"name": "amount", "type": "double"},
		{"name": "count", "type": "int"},
		{"name": "customer_id", "type": "string"},
		{"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["PENDING", "COMPLETED"]}},
		{"name": "items", "type": {"type": "array", "items": "string"}}
	]
}`

func buildTestEnv(t *testing.T) *cel.Env {
	t.Helper()
	m := schema.NewAvroMapper()
	mapped, err := m.Map([]byte(orderSchema))
	if err != nil {
		t.Fatalf("Map error: %v", err)
	}

	adapter, err := schema.NewAvroAdapter(mapped)
	if err != nil {
		t.Fatalf("NewAvroAdapter error: %v", err)
	}

	provider := schema.NewTypeProvider()
	env, _, err := schema.BuildTypedEnv(adapter, provider, schema.DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}
	return env
}

func TestSchemaValidation_ValidExpressions(t *testing.T) {
	env := buildTestEnv(t)

	valid := []string{
		"event.amount > 100.0",
		"event.order_id.startsWith('ORD-')",
		"event.count + 1",
		"event.count > 0 && event.amount > 50.0",
		"event.status == 'COMPLETED'",
		"event.items.size() > 0",
		"event.amount >= 100.0 ? 'high' : 'low'",
	}

	for _, expr := range valid {
		_, issues := env.Compile(expr)
		if issues != nil && issues.Err() != nil {
			t.Errorf("expected valid: %s\n  error: %v", expr, issues.Err())
		}
	}
}

func TestSchemaValidation_InvalidExpressions(t *testing.T) {
	env := buildTestEnv(t)

	invalid := []struct {
		expr        string
		errContains string
	}{
		{"event.amount > 'hundred'", "no matching overload"},
		{"event.nonexistent_field > 0", "undefined field"},
		{"event.order_id + 1", "no matching overload"},
		{"event.count.startsWith('x')", "no matching overload"},
	}

	for _, tt := range invalid {
		_, issues := env.Compile(tt.expr)
		if issues == nil || issues.Err() == nil {
			t.Errorf("expected compile error for: %s", tt.expr)
		}
	}
}

func TestSchemaValidation_CompileAndEval(t *testing.T) {
	env := buildTestEnv(t)

	compiler := cel.NewCompiler(env)
	evaluator := cel.NewEvaluator()

	expr, err := compiler.CompileBool("event.amount > 100.0 && event.status == 'COMPLETED'")
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	tests := []struct {
		amount float64
		status string
		want   bool
	}{
		{150.0, "COMPLETED", true},
		{150.0, "PENDING", false},
		{50.0, "COMPLETED", false},
	}

	for _, tt := range tests {
		vars := map[string]any{
			"event": map[string]any{
				"amount": tt.amount,
				"status": tt.status,
			},
		}
		got, err := evaluator.EvalBool(expr, vars)
		if err != nil {
			t.Errorf("Eval error: %v", err)
			continue
		}
		if got != tt.want {
			t.Errorf("amount=%v status=%s: got %v, want %v", tt.amount, tt.status, got, tt.want)
		}
	}
}

// Benchmarks

func buildBenchEnv(b *testing.B) *cel.Env {
	b.Helper()
	m := schema.NewAvroMapper()
	mapped, _ := m.Map([]byte(orderSchema))
	adapter, _ := schema.NewAvroAdapter(mapped)
	provider := schema.NewTypeProvider()
	env, _, _ := schema.BuildTypedEnv(adapter, provider, schema.DefaultEnvOptions())
	return env
}

func BenchmarkSchemaMap(b *testing.B) {
	m := schema.NewAvroMapper()
	schemaBytes := []byte(orderSchema)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Map(schemaBytes)
	}
}

func BenchmarkBuildTypedEnv(b *testing.B) {
	m := schema.NewAvroMapper()
	mapped, _ := m.Map([]byte(orderSchema))
	adapter, _ := schema.NewAvroAdapter(mapped)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		provider := schema.NewTypeProvider()
		_, _, _ = schema.BuildTypedEnv(adapter, provider, schema.DefaultEnvOptions())
	}
}

func BenchmarkCompile_Simple(b *testing.B) {
	env := buildBenchEnv(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = env.Compile("event.amount > 100.0")
	}
}

func BenchmarkCompile_Complex(b *testing.B) {
	env := buildBenchEnv(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = env.Compile("event.amount > 100.0 && event.status == 'COMPLETED' && event.items.size() > 0")
	}
}

func BenchmarkCompileWithCache(b *testing.B) {
	env := buildBenchEnv(b)
	compiler := cel.NewCompiler(env)
	cache := cel.NewExprCache()

	expr := "event.amount > 100.0 && event.count > 5"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cache.GetOrCompile(expr, compiler.Compile)
	}
}

func BenchmarkEval_Simple(b *testing.B) {
	env := buildBenchEnv(b)
	compiler := cel.NewCompiler(env)
	evaluator := cel.NewEvaluator()

	expr, _ := compiler.CompileBool("event.amount > 100.0")
	vars := map[string]any{
		"event": map[string]any{"amount": 150.0},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = evaluator.EvalBool(expr, vars)
	}
}

func BenchmarkEval_Complex(b *testing.B) {
	env := buildBenchEnv(b)
	compiler := cel.NewCompiler(env)
	evaluator := cel.NewEvaluator()

	expr, _ := compiler.CompileBool("event.amount > 100.0 && event.status == 'COMPLETED' && event.count > 5")
	vars := map[string]any{
		"event": map[string]any{
			"amount": 150.0,
			"status": "COMPLETED",
			"count":  int64(10),
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = evaluator.EvalBool(expr, vars)
	}
}

func BenchmarkEval_StringOps(b *testing.B) {
	env := buildBenchEnv(b)
	compiler := cel.NewCompiler(env)
	evaluator := cel.NewEvaluator()

	expr, _ := compiler.CompileBool("event.order_id.startsWith('ORD-') && event.customer_id.contains('VIP')")
	vars := map[string]any{
		"event": map[string]any{
			"order_id":    "ORD-12345",
			"customer_id": "VIP-USER-001",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = evaluator.EvalBool(expr, vars)
	}
}

func BenchmarkEval_ListOps(b *testing.B) {
	env := buildBenchEnv(b)
	compiler := cel.NewCompiler(env)
	evaluator := cel.NewEvaluator()

	expr, _ := compiler.CompileBool("event.items.size() > 0 && 'apple' in event.items")
	vars := map[string]any{
		"event": map[string]any{
			"items": []any{"apple", "banana", "orange"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = evaluator.EvalBool(expr, vars)
	}
}
