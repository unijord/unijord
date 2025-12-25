package ext

import (
	"testing"

	"github.com/google/cel-go/cel"
)

func TestStringFuncs(t *testing.T) {
	env, err := cel.NewEnv(
		StringFuncs(),
		cel.Variable("s", cel.StringType),
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
			name:     "lower",
			expr:     "lower(s)",
			vars:     map[string]any{"s": "HELLO World"},
			expected: "hello world",
		},
		{
			name:     "upper",
			expr:     "upper(s)",
			vars:     map[string]any{"s": "Hello World"},
			expected: "HELLO WORLD",
		},
		{
			name:     "trim",
			expr:     "trim(s)",
			vars:     map[string]any{"s": "  hello  "},
			expected: "hello",
		},
		{
			name:     "trimPrefix",
			expr:     "trimPrefix(s, 'Hello ')",
			vars:     map[string]any{"s": "Hello World"},
			expected: "World",
		},
		{
			name:     "trimPrefix_no_match",
			expr:     "trimPrefix(s, 'Foo')",
			vars:     map[string]any{"s": "Hello World"},
			expected: "Hello World",
		},
		{
			name:     "trimSuffix",
			expr:     "trimSuffix(s, ' World')",
			vars:     map[string]any{"s": "Hello World"},
			expected: "Hello",
		},
		{
			name:     "replace",
			expr:     "replace(s, 'o', '0')",
			vars:     map[string]any{"s": "Hello World"},
			expected: "Hell0 W0rld",
		},
		{
			name:     "substr",
			expr:     "substr(s, 0, 5)",
			vars:     map[string]any{"s": "Hello World"},
			expected: "Hello",
		},
		{
			name:     "substr_middle",
			expr:     "substr(s, 6, 5)",
			vars:     map[string]any{"s": "Hello World"},
			expected: "World",
		},
		{
			name:     "substr_bounds",
			expr:     "substr(s, 6, 100)",
			vars:     map[string]any{"s": "Hello World"},
			expected: "World",
		},
		{
			name:     "padLeft",
			expr:     "padLeft(s, 8, '0')",
			vars:     map[string]any{"s": "123"},
			expected: "00000123",
		},
		{
			name:     "padRight",
			expr:     "padRight(s, 8, '*')",
			vars:     map[string]any{"s": "hello"},
			expected: "hello***",
		},
		{
			name:     "regexMatch_true",
			expr:     "regexMatch(s, '^[0-9]+$')",
			vars:     map[string]any{"s": "12345"},
			expected: true,
		},
		{
			name:     "regexMatch_false",
			expr:     "regexMatch(s, '^[0-9]+$')",
			vars:     map[string]any{"s": "12345abc"},
			expected: false,
		},
		{
			name:     "regexReplace",
			expr:     "regexReplace(s, '[0-9]+', 'X')",
			vars:     map[string]any{"s": "abc123def456"},
			expected: "abcXdefX",
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

func TestSplitJoin(t *testing.T) {
	env, err := cel.NewEnv(
		StringFuncs(),
		cel.Variable("s", cel.StringType),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	ast, issues := env.Compile("split(s, ',')")
	if issues != nil && issues.Err() != nil {
		t.Fatalf("Compile: %v", issues.Err())
	}

	prog, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program: %v", err)
	}

	out, _, err := prog.Eval(map[string]any{"s": "a,b,c"})
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}

	result := out.Value().([]string)
	if len(result) != 3 || result[0] != "a" || result[1] != "b" || result[2] != "c" {
		t.Errorf("split: got %v, want [a b c]", result)
	}

	ast2, issues := env.Compile("join(split(s, ','), '-')")
	if issues != nil && issues.Err() != nil {
		t.Fatalf("Compile join: %v", issues.Err())
	}

	prog2, err := env.Program(ast2)
	if err != nil {
		t.Fatalf("Program: %v", err)
	}

	out2, _, err := prog2.Eval(map[string]any{"s": "a,b,c"})
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}

	if out2.Value() != "a-b-c" {
		t.Errorf("join: got %v, want a-b-c", out2.Value())
	}
}

func TestRegexMatchInvalidPattern(t *testing.T) {
	env, err := cel.NewEnv(
		StringFuncs(),
		cel.Variable("s", cel.StringType),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	ast, issues := env.Compile("regexMatch(s, '[invalid')")
	if issues != nil && issues.Err() != nil {
		t.Fatalf("Compile: %v", issues.Err())
	}

	prog, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program: %v", err)
	}

	_, _, err = prog.Eval(map[string]any{"s": "test"})
	if err == nil {
		t.Error("expected error for invalid regex pattern")
	}
}

func TestStringFuncs_Mask(t *testing.T) {
	env, err := cel.NewEnv(
		StringFuncs(),
		cel.Variable("s", cel.StringType),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	tests := []struct {
		name     string
		expr     string
		vars     map[string]any
		expected string
	}{
		{
			name:     "credit_card",
			expr:     "mask(s, 4, 4)",
			vars:     map[string]any{"s": "4111222233334444"},
			expected: "4111********4444",
		},
		{
			name: "email",
			expr: "mask(s, 2, 4)",
			vars: map[string]any{"s": "john@example.com"},
			// 16 chars, show first 2 + last 4, mask 10
			expected: "jo**********.com",
		},
		{
			name: "short_string",
			expr: "mask(s, 4, 4)",
			vars: map[string]any{"s": "short"},
			// too short to mask
			expected: "short",
		},
		{
			name:     "show_first_only",
			expr:     "mask(s, 3, 0)",
			vars:     map[string]any{"s": "secret123"},
			expected: "sec******",
		},
		{
			name:     "show_last_only",
			expr:     "mask(s, 0, 4)",
			vars:     map[string]any{"s": "secret1234"},
			expected: "******1234",
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
				t.Errorf("got %q, want %q", out.Value(), tc.expected)
			}
		})
	}
}

func TestStringFuncs_Truncate(t *testing.T) {
	env, err := cel.NewEnv(
		StringFuncs(),
		cel.Variable("s", cel.StringType),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	tests := []struct {
		name     string
		expr     string
		vars     map[string]any
		expected string
	}{
		{
			name:     "long_string",
			expr:     "truncate(s, 10)",
			vars:     map[string]any{"s": "This is a very long string that needs truncation"},
			expected: "This is...",
		},
		{
			name:     "short_string",
			expr:     "truncate(s, 20)",
			vars:     map[string]any{"s": "Short"},
			expected: "Short",
		},
		{
			name:     "exact_length",
			expr:     "truncate(s, 5)",
			vars:     map[string]any{"s": "Hello"},
			expected: "Hello",
		},
		{
			name:     "very_short_max",
			expr:     "truncate(s, 3)",
			vars:     map[string]any{"s": "Hello World"},
			expected: "Hel",
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
				t.Errorf("got %q, want %q", out.Value(), tc.expected)
			}
		})
	}
}

func TestStringFuncs_ExtractDomain(t *testing.T) {
	env, err := cel.NewEnv(
		StringFuncs(),
		cel.Variable("email", cel.StringType),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	tests := []struct {
		name     string
		expr     string
		vars     map[string]any
		expected string
	}{
		{
			name:     "simple_email",
			expr:     "extractDomain(email)",
			vars:     map[string]any{"email": "john@example.com"},
			expected: "example.com",
		},
		{
			name:     "subdomain",
			expr:     "extractDomain(email)",
			vars:     map[string]any{"email": "user@mail.company.co.uk"},
			expected: "mail.company.co.uk",
		},
		{
			name:     "no_at",
			expr:     "extractDomain(email)",
			vars:     map[string]any{"email": "not-an-email"},
			expected: "",
		},
		{
			name:     "trailing_at",
			expr:     "extractDomain(email)",
			vars:     map[string]any{"email": "user@"},
			expected: "",
		},
		{
			name:     "multiple_at",
			expr:     "extractDomain(email)",
			vars:     map[string]any{"email": "user@first@second.com"},
			expected: "second.com",
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
				t.Errorf("got %q, want %q", out.Value(), tc.expected)
			}
		})
	}
}
