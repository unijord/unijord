package ext

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

func TestListFuncs(t *testing.T) {
	env, err := cel.NewEnv(
		ListFuncs(),
		cel.Variable("ints", cel.ListType(cel.IntType)),
		cel.Variable("doubles", cel.ListType(cel.DoubleType)),
		cel.Variable("strings", cel.ListType(cel.StringType)),
		cel.Variable("nested", cel.ListType(cel.ListType(cel.IntType))),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	tests := []struct {
		name     string
		expr     string
		vars     map[string]any
		expected any
	}{
		{
			name:     "sum_ints",
			expr:     "sum(ints)",
			vars:     map[string]any{"ints": []int64{1, 2, 3, 4, 5}},
			expected: int64(15),
		},
		{
			name:     "sum_empty",
			expr:     "sum(ints)",
			vars:     map[string]any{"ints": []int64{}},
			expected: int64(0),
		},
		{
			name:     "sumDouble",
			expr:     "sumDouble(doubles)",
			vars:     map[string]any{"doubles": []float64{1.5, 2.5, 3.0}},
			expected: 7.0,
		},
		{
			name:     "min_ints",
			expr:     "min(ints)",
			vars:     map[string]any{"ints": []int64{5, 2, 8, 1, 9}},
			expected: int64(1),
		},
		{
			name:     "minDouble",
			expr:     "minDouble(doubles)",
			vars:     map[string]any{"doubles": []float64{3.5, 1.2, 4.8}},
			expected: 1.2,
		},
		{
			name:     "max_ints",
			expr:     "max(ints)",
			vars:     map[string]any{"ints": []int64{5, 2, 8, 1, 9}},
			expected: int64(9),
		},
		{
			name:     "maxDouble",
			expr:     "maxDouble(doubles)",
			vars:     map[string]any{"doubles": []float64{3.5, 1.2, 4.8}},
			expected: 4.8,
		},
		{
			name:     "first_ints",
			expr:     "first(ints)",
			vars:     map[string]any{"ints": []int64{10, 20, 30}},
			expected: int64(10),
		},
		{
			name:     "first_strings",
			expr:     "first(strings)",
			vars:     map[string]any{"strings": []string{"a", "b", "c"}},
			expected: "a",
		},
		{
			name:     "last_ints",
			expr:     "last(ints)",
			vars:     map[string]any{"ints": []int64{10, 20, 30}},
			expected: int64(30),
		},
		{
			name:     "last_strings",
			expr:     "last(strings)",
			vars:     map[string]any{"strings": []string{"a", "b", "c"}},
			expected: "c",
		},
		{
			name:     "avg_ints",
			expr:     "avg(ints)",
			vars:     map[string]any{"ints": []int64{2, 4, 6}},
			expected: 4.0,
		},
		{
			name:     "avgDouble",
			expr:     "avgDouble(doubles)",
			vars:     map[string]any{"doubles": []float64{1.0, 2.0, 3.0}},
			expected: 2.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ast, issues := env.Compile(tc.expr)
			if issues != nil && issues.Err() != nil {
				t.Fatalf("Compile(%q): %v", tc.expr, issues.Err())
			}

			prog, err := env.Program(ast)
			if err != nil {
				t.Fatalf("Program: %v", err)
			}

			out, _, err := prog.Eval(tc.vars)
			if err != nil {
				t.Fatalf("Eval: %v", err)
			}

			if out.Value() != tc.expected {
				t.Errorf("got %v (%T), want %v (%T)", out.Value(), out.Value(), tc.expected, tc.expected)
			}
		})
	}
}

func TestListFuncs_EmptyList(t *testing.T) {
	env, err := cel.NewEnv(
		ListFuncs(),
		cel.Variable("ints", cel.ListType(cel.IntType)),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	// first/last on empty list returns null
	ast, _ := env.Compile("first(ints)")
	prog, _ := env.Program(ast)
	out, _, _ := prog.Eval(map[string]any{"ints": []int64{}})
	if out.Type() != types.NullType {
		t.Errorf("first(empty): expected null, got %v", out.Value())
	}

	ast2, _ := env.Compile("last(ints)")
	prog2, _ := env.Program(ast2)
	out2, _, _ := prog2.Eval(map[string]any{"ints": []int64{}})
	if out2.Type() != types.NullType {
		t.Errorf("last(empty): expected null, got %v", out2.Value())
	}

	// min/max on empty list returns error
	ast3, _ := env.Compile("min(ints)")
	prog3, _ := env.Program(ast3)
	_, _, err = prog3.Eval(map[string]any{"ints": []int64{}})
	if err == nil {
		t.Error("min(empty): expected error")
	}
}

func TestListFuncs_Flatten(t *testing.T) {
	env, err := cel.NewEnv(
		ListFuncs(),
		cel.Variable("nested", cel.ListType(cel.ListType(cel.IntType))),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	ast, issues := env.Compile("flatten(nested)")
	if issues != nil && issues.Err() != nil {
		t.Fatalf("Compile: %v", issues.Err())
	}

	prog, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program: %v", err)
	}

	out, _, err := prog.Eval(map[string]any{
		"nested": [][]int64{{1, 2}, {3, 4, 5}, {6}},
	})
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}

	result := out.Value().([]ref.Val)
	if len(result) != 6 {
		t.Errorf("flatten: got %d elements, want 6", len(result))
	}
}

func TestListFuncs_AggregateBy(t *testing.T) {
	env, err := cel.NewEnv(
		ListFuncs(),
		cel.Variable("items", cel.ListType(cel.DynType)),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	items := []map[string]any{
		{"name": "Widget", "price": 10.50, "quantity": int64(2)},
		{"name": "Gadget", "price": 25.00, "quantity": int64(1)},
		{"name": "Gizmo", "price": 15.75, "quantity": int64(3)},
	}

	tests := []struct {
		name     string
		expr     string
		expected float64
	}{
		{
			name: "sumBy_price",
			expr: `sumBy(items, "price")`,
			// 10.50 + 25.00 + 15.75
			expected: 51.25,
		},
		{
			name: "sumBy_quantity",
			expr: `sumBy(items, "quantity")`,
			// 2 + 1 + 3
			expected: 6.0,
		},
		{
			name: "avgBy_price",
			expr: `avgBy(items, "price")`,
			// 51.25 / 3
			expected: 17.083333333333332,
		},
		{
			name:     "minBy_price",
			expr:     `minBy(items, "price")`,
			expected: 10.50,
		},
		{
			name:     "maxBy_price",
			expr:     `maxBy(items, "price")`,
			expected: 25.00,
		},
		{
			name:     "minBy_quantity",
			expr:     `minBy(items, "quantity")`,
			expected: 1.0,
		},
		{
			name:     "maxBy_quantity",
			expr:     `maxBy(items, "quantity")`,
			expected: 3.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ast, issues := env.Compile(tc.expr)
			if issues != nil && issues.Err() != nil {
				t.Fatalf("Compile(%q): %v", tc.expr, issues.Err())
			}

			prog, err := env.Program(ast)
			if err != nil {
				t.Fatalf("Program: %v", err)
			}

			out, _, err := prog.Eval(map[string]any{"items": items})
			if err != nil {
				t.Fatalf("Eval: %v", err)
			}

			got := out.Value().(float64)
			if got != tc.expected {
				t.Errorf("got %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestListFuncs_AggregateBy_NestedField(t *testing.T) {
	env, err := cel.NewEnv(
		ListFuncs(),
		cel.Variable("orders", cel.ListType(cel.DynType)),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	orders := []map[string]any{
		{"id": "ord-1", "pricing": map[string]any{"total": 100.0, "tax": 10.0}},
		{"id": "ord-2", "pricing": map[string]any{"total": 200.0, "tax": 20.0}},
		{"id": "ord-3", "pricing": map[string]any{"total": 150.0, "tax": 15.0}},
	}

	tests := []struct {
		name     string
		expr     string
		expected float64
	}{
		{
			name:     "sumBy_nested_total",
			expr:     `sumBy(orders, "pricing.total")`,
			expected: 450.0,
		},
		{
			name:     "sumBy_nested_tax",
			expr:     `sumBy(orders, "pricing.tax")`,
			expected: 45.0,
		},
		{
			name:     "avgBy_nested_total",
			expr:     `avgBy(orders, "pricing.total")`,
			expected: 150.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ast, issues := env.Compile(tc.expr)
			if issues != nil && issues.Err() != nil {
				t.Fatalf("Compile(%q): %v", tc.expr, issues.Err())
			}

			prog, err := env.Program(ast)
			if err != nil {
				t.Fatalf("Program: %v", err)
			}

			out, _, err := prog.Eval(map[string]any{"orders": orders})
			if err != nil {
				t.Fatalf("Eval: %v", err)
			}

			got := out.Value().(float64)
			if got != tc.expected {
				t.Errorf("got %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestListFuncs_AggregateBy_EmptyList(t *testing.T) {
	env, err := cel.NewEnv(
		ListFuncs(),
		cel.Variable("items", cel.ListType(cel.DynType)),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	emptyItems := []map[string]any{}

	tests := []struct {
		name     string
		expr     string
		checkNaN bool
		expected float64
	}{
		{
			name:     "sumBy_empty",
			expr:     `sumBy(items, "price")`,
			expected: 0.0,
		},
		{
			name:     "minBy_empty",
			expr:     `minBy(items, "price")`,
			expected: 0.0,
		},
		{
			name:     "maxBy_empty",
			expr:     `maxBy(items, "price")`,
			expected: 0.0,
		},
		{
			name:     "avgBy_empty",
			expr:     `avgBy(items, "price")`,
			checkNaN: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ast, issues := env.Compile(tc.expr)
			if issues != nil && issues.Err() != nil {
				t.Fatalf("Compile(%q): %v", tc.expr, issues.Err())
			}

			prog, err := env.Program(ast)
			if err != nil {
				t.Fatalf("Program: %v", err)
			}

			out, _, err := prog.Eval(map[string]any{"items": emptyItems})
			if err != nil {
				t.Fatalf("Eval: %v", err)
			}

			got := out.Value().(float64)
			if tc.checkNaN {
				if got == got {
					t.Errorf("expected NaN, got %v", got)
				}
			} else if got != tc.expected {
				t.Errorf("got %v, want %v", got, tc.expected)
			}
		})
	}
}
