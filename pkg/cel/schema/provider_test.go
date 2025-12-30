package schema

import (
	"testing"
)

var nestedSchema = `{
	"type": "record",
	"name": "Order",
	"namespace": "com.example",
	"fields": [
		{"name": "order_id", "type": "string"},
		{"name": "amount", "type": "double"},
		{"name": "customer", "type": {
			"type": "record",
			"name": "Customer",
			"fields": [
				{"name": "name", "type": "string"},
				{"name": "age", "type": "int"},
				{"name": "address", "type": {
					"type": "record",
					"name": "Address",
					"fields": [
						{"name": "city", "type": "string"},
						{"name": "geo", "type": {
							"type": "record",
							"name": "Geo",
							"fields": [
								{"name": "lat", "type": "double"},
								{"name": "lng", "type": "double"}
							]
						}}
					]
				}}
			]
		}},
		{"name": "items", "type": {"type": "array", "items": {
			"type": "record",
			"name": "Item",
			"fields": [
				{"name": "sku", "type": "string"},
				{"name": "price", "type": "double"},
				{"name": "qty", "type": "int"}
			]
		}}},
		{"name": "metadata", "type": {"type": "map", "values": "string"}}
	]
}`

func TestTypeProvider_RegistersAllTypes(t *testing.T) {
	m := NewAvroMapper()
	mapped, err := m.Map([]byte(nestedSchema))
	if err != nil {
		t.Fatalf("Map error: %v", err)
	}

	adapter, err := NewAvroAdapter(mapped)
	if err != nil {
		t.Fatalf("NewAvroAdapter error: %v", err)
	}

	provider := NewTypeProvider()
	_, rootType, err := BuildTypedEnv(adapter, provider, DefaultEnvOptions())
	if err != nil {
		t.Fatalf("BuildTypedEnv error: %v", err)
	}

	if rootType != "com.example.Order" {
		t.Errorf("rootType = %s, want com.example.Order", rootType)
	}

	expectedTypes := []string{
		"com.example.Order",
		"com.example.Customer",
		"com.example.Address",
		"com.example.Geo",
		"com.example.Item",
	}

	for _, typeName := range expectedTypes {
		if _, ok := provider.FindStructType(typeName); !ok {
			t.Errorf("type %s not registered", typeName)
		}
	}
}

func TestTypeProvider_ValidExpressions(t *testing.T) {
	m := NewAvroMapper()
	mapped, _ := m.Map([]byte(nestedSchema))
	adapter, _ := NewAvroAdapter(mapped)
	provider := NewTypeProvider()
	env, _, _ := BuildTypedEnv(adapter, provider, DefaultEnvOptions())

	valid := []string{
		"event.amount > 100.0",
		"event.order_id.startsWith('ORD')",

		"event.customer.name == 'John'",
		"event.customer.age > 18",

		"event.customer.address.city == 'NYC'",
		"event.customer.address.geo.lat > 40.0",
		"event.customer.address.geo.lat > 40.0 && event.customer.address.geo.lng < -70.0",

		"event.items.size() > 0",
		"event.items[0].price > 10.0",
		"event.items.filter(i, i.price > 5.0).size() > 0",
		"event.items.map(i, i.price * double(i.qty))",

		"event.metadata['region'] == 'US'",
		"'region' in event.metadata",
	}

	for _, expr := range valid {
		_, issues := env.Compile(expr)
		if issues != nil && issues.Err() != nil {
			t.Errorf("expected valid: %s\n  error: %v", expr, issues.Err())
		}
	}
}

func TestTypeProvider_InvalidExpressions(t *testing.T) {
	m := NewAvroMapper()
	mapped, _ := m.Map([]byte(nestedSchema))
	adapter, _ := NewAvroAdapter(mapped)
	provider := NewTypeProvider()
	env, _, _ := BuildTypedEnv(adapter, provider, DefaultEnvOptions())

	invalid := []struct {
		expr    string
		errType string
	}{
		{"event.amountttt > 100.0", "undefined field"},

		{"event.customer.ageeee > 18", "undefined field"},
		{"event.customer.address.cityyy == 'NYC'", "undefined field"},
		{"event.customer.address.geo.lattt > 40.0", "undefined field"},

		{"event.amount > 'hundred'", "no matching overload"},
		{"event.customer.age > 'eighteen'", "no matching overload"},
		{"event.customer.address.geo.lat > 'bad'", "no matching overload"},

		{"event.items[0].priceeee > 10.0", "undefined field"},
		{"event.items.filter(i, i.price > 'bad')", "no matching overload"},
	}

	for _, tt := range invalid {
		_, issues := env.Compile(tt.expr)
		if issues == nil || issues.Err() == nil {
			t.Errorf("expected compile error for: %s", tt.expr)
		}
	}
}

func TestTypeProvider_RuntimeEval(t *testing.T) {
	m := NewAvroMapper()
	mapped, _ := m.Map([]byte(nestedSchema))
	adapter, _ := NewAvroAdapter(mapped)
	provider := NewTypeProvider()
	env, _, _ := BuildTypedEnv(adapter, provider, DefaultEnvOptions())

	ast, issues := env.Compile("event.customer.address.geo.lat > 40.0 && event.items.size() > 0")
	if issues != nil && issues.Err() != nil {
		t.Fatalf("Compile error: %v", issues.Err())
	}

	prog, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program error: %v", err)
	}

	data := map[string]any{
		"event": map[string]any{
			"order_id": "ORD-123",
			"amount":   150.0,
			"customer": map[string]any{
				"name": "John",
				"age":  int64(30),
				"address": map[string]any{
					"city": "NYC",
					"geo": map[string]any{
						"lat": 40.7128,
						"lng": -74.0060,
					},
				},
			},
			"items": []any{
				map[string]any{"sku": "A1", "price": 10.0, "qty": int64(2)},
			},
			"metadata": map[string]any{"region": "US"},
		},
	}

	out, _, err := prog.Eval(data)
	if err != nil {
		t.Fatalf("Eval error: %v", err)
	}
	result, ok := out.Value().(bool)
	if !ok {
		t.Fatalf("expected bool, got %T", out.Value())
	}
	if !result {
		t.Error("expected true")
	}
}

func BenchmarkTypedEnv_Compile(b *testing.B) {
	m := NewAvroMapper()
	mapped, _ := m.Map([]byte(nestedSchema))
	adapter, _ := NewAvroAdapter(mapped)
	provider := NewTypeProvider()
	env, _, _ := BuildTypedEnv(adapter, provider, DefaultEnvOptions())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.Compile("event.customer.address.geo.lat > 40.0")
	}
}

func BenchmarkTypedEnv_Eval(b *testing.B) {
	m := NewAvroMapper()
	mapped, _ := m.Map([]byte(nestedSchema))
	adapter, _ := NewAvroAdapter(mapped)
	provider := NewTypeProvider()
	env, _, _ := BuildTypedEnv(adapter, provider, DefaultEnvOptions())

	ast, _ := env.Compile("event.customer.address.geo.lat > 40.0 && event.customer.age > 18")
	prog, _ := env.Program(ast)

	data := map[string]any{
		"event": map[string]any{
			"customer": map[string]any{
				"age": int64(30),
				"address": map[string]any{
					"geo": map[string]any{"lat": 40.7128},
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prog.Eval(data)
	}
}
