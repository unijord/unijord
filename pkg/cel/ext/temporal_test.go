package ext

import (
	"testing"
	"time"

	"github.com/google/cel-go/cel"
)

func TestTemporalFuncs(t *testing.T) {
	env, err := cel.NewEnv(TemporalFuncs())
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
			name:     "year",
			expr:     "year(ts)",
			vars:     map[string]any{"ts": testTime},
			expected: int64(2024),
		},
		{
			name:     "month",
			expr:     "month(ts)",
			vars:     map[string]any{"ts": testTime},
			expected: int64(6),
		},
		{
			name:     "day",
			expr:     "day(ts)",
			vars:     map[string]any{"ts": testTime},
			expected: int64(15),
		},
		{
			name:     "hour",
			expr:     "hour(ts)",
			vars:     map[string]any{"ts": testTime},
			expected: int64(14),
		},
		{
			name:     "minute",
			expr:     "minute(ts)",
			vars:     map[string]any{"ts": testTime},
			expected: int64(30),
		},
		{
			name:     "second",
			expr:     "second(ts)",
			vars:     map[string]any{"ts": testTime},
			expected: int64(45),
		},
		{
			name:     "dayOfWeek",
			expr:     "dayOfWeek(ts)",
			vars:     map[string]any{"ts": testTime},
			expected: int64(6), // Saturday
		},
		{
			name:     "dayOfYear",
			expr:     "dayOfYear(ts)",
			vars:     map[string]any{"ts": testTime},
			expected: int64(167),
		},
		{
			name:     "weekOfYear",
			expr:     "weekOfYear(ts)",
			vars:     map[string]any{"ts": testTime},
			expected: int64(24),
		},
		{
			name:     "date",
			expr:     "date(ts)",
			vars:     map[string]any{"ts": testTime},
			expected: "2024-06-15",
		},
		{
			name:     "epochMillis",
			expr:     "epochMillis(ts)",
			vars:     map[string]any{"ts": testTime},
			expected: testTime.UnixMilli(),
		},
		{
			name:     "fromEpochMillis",
			expr:     "year(fromEpochMillis(ms))",
			vars:     map[string]any{"ms": testTime.UnixMilli()},
			expected: int64(2024),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			envWithVars, err := env.Extend(
				cel.Variable("ts", cel.TimestampType),
				cel.Variable("ms", cel.IntType),
			)
			if err != nil {
				t.Fatalf("Extend: %v", err)
			}

			ast, issues := envWithVars.Compile(tc.expr)
			if issues != nil && issues.Err() != nil {
				t.Fatalf("Compile(%q): %v", tc.expr, issues.Err())
			}

			prog, err := envWithVars.Program(ast)
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

func TestParseTimestamp(t *testing.T) {
	env, err := cel.NewEnv(
		TemporalFuncs(),
		cel.Variable("dateStr", cel.StringType),
		cel.Variable("layout", cel.StringType),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	ast, issues := env.Compile("year(parseTimestamp(dateStr, layout))")
	if issues != nil && issues.Err() != nil {
		t.Fatalf("Compile: %v", issues.Err())
	}

	prog, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program: %v", err)
	}

	out, _, err := prog.Eval(map[string]any{
		"dateStr": "2024-06-15",
		"layout":  "2006-01-02",
	})
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}

	if out.Value() != int64(2024) {
		t.Errorf("got %v, want 2024", out.Value())
	}
}

func TestParseTimestampInvalidFormat(t *testing.T) {
	env, err := cel.NewEnv(
		TemporalFuncs(),
		cel.Variable("dateStr", cel.StringType),
		cel.Variable("layout", cel.StringType),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	ast, issues := env.Compile("parseTimestamp(dateStr, layout)")
	if issues != nil && issues.Err() != nil {
		t.Fatalf("Compile: %v", issues.Err())
	}

	prog, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program: %v", err)
	}

	_, _, err = prog.Eval(map[string]any{
		"dateStr": "not-a-date",
		"layout":  "2006-01-02",
	})
	if err == nil {
		t.Error("expected error for invalid date format")
	}
}
