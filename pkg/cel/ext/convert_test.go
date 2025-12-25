package ext

import (
	"testing"

	"github.com/google/cel-go/cel"
)

func TestConvertFuncs(t *testing.T) {
	env, err := cel.NewEnv(
		ConvertFuncs(),
		cel.Variable("s", cel.StringType),
		cel.Variable("i", cel.IntType),
		cel.Variable("d", cel.DoubleType),
		cel.Variable("b", cel.BoolType),
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
			name:     "toInt_string",
			expr:     "toInt(s)",
			vars:     map[string]any{"s": "42"},
			expected: int64(42),
		},
		{
			name:     "toInt_string_negative",
			expr:     "toInt(s)",
			vars:     map[string]any{"s": "-123"},
			expected: int64(-123),
		},
		{
			name:     "toInt_double",
			expr:     "toInt(d)",
			vars:     map[string]any{"d": 3.7},
			expected: int64(3),
		},
		{
			name:     "toInt_double_negative",
			expr:     "toInt(d)",
			vars:     map[string]any{"d": -2.9},
			expected: int64(-2),
		},
		{
			name:     "toInt_bool_true",
			expr:     "toInt(b)",
			vars:     map[string]any{"b": true},
			expected: int64(1),
		},
		{
			name:     "toInt_bool_false",
			expr:     "toInt(b)",
			vars:     map[string]any{"b": false},
			expected: int64(0),
		},

		{
			name:     "toDouble_string",
			expr:     "toDouble(s)",
			vars:     map[string]any{"s": "3.14"},
			expected: 3.14,
		},
		{
			name:     "toDouble_int",
			expr:     "toDouble(i)",
			vars:     map[string]any{"i": int64(42)},
			expected: 42.0,
		},
		
		{
			name:     "toString_int",
			expr:     "toString(i)",
			vars:     map[string]any{"i": int64(42)},
			expected: "42",
		},
		{
			name:     "toString_double",
			expr:     "toString(d)",
			vars:     map[string]any{"d": 3.14},
			expected: "3.14",
		},
		{
			name:     "toString_bool_true",
			expr:     "toString(b)",
			vars:     map[string]any{"b": true},
			expected: "true",
		},
		{
			name:     "toString_bool_false",
			expr:     "toString(b)",
			vars:     map[string]any{"b": false},
			expected: "false",
		},

		// toBool
		{
			name:     "toBool_string_true",
			expr:     "toBool(s)",
			vars:     map[string]any{"s": "true"},
			expected: true,
		},
		{
			name:     "toBool_string_false",
			expr:     "toBool(s)",
			vars:     map[string]any{"s": "false"},
			expected: false,
		},
		{
			name:     "toBool_string_1",
			expr:     "toBool(s)",
			vars:     map[string]any{"s": "1"},
			expected: true,
		},
		{
			name:     "toBool_string_0",
			expr:     "toBool(s)",
			vars:     map[string]any{"s": "0"},
			expected: false,
		},
		{
			name:     "toBool_int_nonzero",
			expr:     "toBool(i)",
			vars:     map[string]any{"i": int64(42)},
			expected: true,
		},
		{
			name:     "toBool_int_zero",
			expr:     "toBool(i)",
			vars:     map[string]any{"i": int64(0)},
			expected: false,
		},
		{
			name:     "toBool_int_negative",
			expr:     "toBool(i)",
			vars:     map[string]any{"i": int64(-1)},
			expected: true,
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

func TestConvertFuncs_Errors(t *testing.T) {
	env, err := cel.NewEnv(
		ConvertFuncs(),
		cel.Variable("s", cel.StringType),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	errorCases := []struct {
		name string
		expr string
		vars map[string]any
	}{
		{
			name: "toInt_invalid_string",
			expr: "toInt(s)",
			vars: map[string]any{"s": "not-a-number"},
		},
		{
			name: "toDouble_invalid_string",
			expr: "toDouble(s)",
			vars: map[string]any{"s": "not-a-number"},
		},
		{
			name: "toBool_invalid_string",
			expr: "toBool(s)",
			vars: map[string]any{"s": "maybe"},
		},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			ast, issues := env.Compile(tc.expr)
			if issues != nil && issues.Err() != nil {
				t.Fatalf("Compile(%q): %v", tc.expr, issues.Err())
			}

			prog, err := env.Program(ast)
			if err != nil {
				t.Fatalf("Program: %v", err)
			}

			_, _, err = prog.Eval(tc.vars)
			if err == nil {
				t.Error("expected error for invalid conversion")
			}
		})
	}
}
