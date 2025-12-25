package ext

import (
	"testing"
	"time"

	"github.com/google/cel-go/cel"
)

func TestAllFuncs(t *testing.T) {
	opts := append(AllFuncs(),
		cel.Variable("ts", cel.TimestampType),
		cel.Variable("s", cel.StringType),
		cel.Variable("a", cel.DynType),
		cel.Variable("b", cel.DynType),
	)

	env, err := cel.NewEnv(opts...)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	testTime := time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC)

	tests := []struct {
		name     string
		expr     string
		vars     map[string]any
		expected any
	}{
		{
			name:     "temporal_year",
			expr:     "year(ts)",
			vars:     map[string]any{"ts": testTime, "s": "", "a": nil, "b": nil},
			expected: int64(2024),
		},
		{
			name:     "string_upper",
			expr:     "upper(s)",
			vars:     map[string]any{"ts": testTime, "s": "hello", "a": nil, "b": nil},
			expected: "HELLO",
		},
		{
			name:     "null_coalesce",
			expr:     "coalesce(a, b)",
			vars:     map[string]any{"ts": testTime, "s": "", "a": nil, "b": "default"},
			expected: "default",
		},
		{
			name:     "combined_date_upper",
			expr:     "upper(date(ts))",
			vars:     map[string]any{"ts": testTime, "s": "", "a": nil, "b": nil},
			expected: "2024-06-15",
		},
		{
			name:     "combined_ifnull_upper",
			expr:     "upper(ifNull(a, s))",
			vars:     map[string]any{"ts": testTime, "s": "fallback", "a": nil, "b": nil},
			expected: "FALLBACK",
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

func TestAllFuncsLibraryNames(t *testing.T) {
	opts := AllFuncs()
	if len(opts) != 6 {
		t.Errorf("AllFuncs returned %d options, want 6", len(opts))
	}
}
