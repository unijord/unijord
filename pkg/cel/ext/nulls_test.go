package ext

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
)

func TestNullFuncs(t *testing.T) {
	env, err := cel.NewEnv(
		NullFuncs(),
		cel.Variable("a", cel.DynType),
		cel.Variable("b", cel.DynType),
		cel.Variable("c", cel.DynType),
		cel.Variable("d", cel.DynType),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	tests := []struct {
		name       string
		expr       string
		vars       map[string]any
		expected   any
		expectNull bool
	}{
		{
			name:     "coalesce_first_non_null",
			expr:     "coalesce(a, b)",
			vars:     map[string]any{"a": "hello", "b": "world"},
			expected: "hello",
		},
		{
			name:     "coalesce_second_when_first_null",
			expr:     "coalesce(a, b)",
			vars:     map[string]any{"a": nil, "b": "world"},
			expected: "world",
		},
		{
			name:       "coalesce_both_null",
			expr:       "coalesce(a, b)",
			vars:       map[string]any{"a": nil, "b": nil},
			expectNull: true,
		},
		{
			name:     "coalesce3_first",
			expr:     "coalesce3(a, b, c)",
			vars:     map[string]any{"a": "first", "b": "second", "c": "third"},
			expected: "first",
		},
		{
			name:     "coalesce3_second",
			expr:     "coalesce3(a, b, c)",
			vars:     map[string]any{"a": nil, "b": "second", "c": "third"},
			expected: "second",
		},
		{
			name:     "coalesce3_third",
			expr:     "coalesce3(a, b, c)",
			vars:     map[string]any{"a": nil, "b": nil, "c": "third"},
			expected: "third",
		},
		{
			name:     "coalesce4_fourth",
			expr:     "coalesce4(a, b, c, d)",
			vars:     map[string]any{"a": nil, "b": nil, "c": nil, "d": "fourth"},
			expected: "fourth",
		},
		{
			name:     "ifNull_value_non_null",
			expr:     "ifNull(a, b)",
			vars:     map[string]any{"a": "value", "b": "default"},
			expected: "value",
		},
		{
			name:     "ifNull_value_null",
			expr:     "ifNull(a, b)",
			vars:     map[string]any{"a": nil, "b": "default"},
			expected: "default",
		},
		{
			name:     "orDefault_value_non_null",
			expr:     "orDefault(a, b)",
			vars:     map[string]any{"a": int64(42), "b": int64(0)},
			expected: int64(42),
		},
		{
			name:     "orDefault_value_null",
			expr:     "orDefault(a, b)",
			vars:     map[string]any{"a": nil, "b": int64(0)},
			expected: int64(0),
		},
		{
			name:     "isNull_true",
			expr:     "isNull(a)",
			vars:     map[string]any{"a": nil},
			expected: true,
		},
		{
			name:     "isNull_false",
			expr:     "isNull(a)",
			vars:     map[string]any{"a": "hello"},
			expected: false,
		},
		{
			name:     "isNotNull_true",
			expr:     "isNotNull(a)",
			vars:     map[string]any{"a": "hello"},
			expected: true,
		},
		{
			name:     "isNotNull_false",
			expr:     "isNotNull(a)",
			vars:     map[string]any{"a": nil},
			expected: false,
		},
		{
			name:       "nullIf_equal",
			expr:       "nullIf(a, b)",
			vars:       map[string]any{"a": "test", "b": "test"},
			expectNull: true,
		},
		{
			name:     "nullIf_not_equal",
			expr:     "nullIf(a, b)",
			vars:     map[string]any{"a": "hello", "b": "test"},
			expected: "hello",
		},
		{
			name:       "nullIf_int_equal",
			expr:       "nullIf(a, b)",
			vars:       map[string]any{"a": int64(0), "b": int64(0)},
			expectNull: true,
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

			if tc.expectNull {
				if out.Type() != types.NullType {
					t.Errorf("expected null, got %v (%T)", out.Value(), out.Value())
				}
			} else if out.Value() != tc.expected {
				t.Errorf("got %v (%T), want %v (%T)", out.Value(), out.Value(), tc.expected, tc.expected)
			}
		})
	}
}

func TestNullFuncsWithTypes(t *testing.T) {
	env, err := cel.NewEnv(
		NullFuncs(),
		cel.Variable("val", cel.DynType),
		cel.Variable("def", cel.DynType),
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
			name:     "ifNull_with_int",
			expr:     "ifNull(val, def)",
			vars:     map[string]any{"val": nil, "def": int64(100)},
			expected: int64(100),
		},
		{
			name:     "ifNull_with_bool",
			expr:     "ifNull(val, def)",
			vars:     map[string]any{"val": nil, "def": true},
			expected: true,
		},
		{
			name:     "ifNull_with_float",
			expr:     "ifNull(val, def)",
			vars:     map[string]any{"val": nil, "def": 3.14},
			expected: 3.14,
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
