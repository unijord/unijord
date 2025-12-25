package ext

import (
	"testing"

	"github.com/google/cel-go/cel"
)

func TestCryptoFuncs(t *testing.T) {
	env, err := cel.NewEnv(
		CryptoFuncs(),
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
			name:     "xxhash",
			expr:     "xxhash(s)",
			vars:     map[string]any{"s": "hello"},
			expected: "26c7827d889f6da3",
		},
		{
			name:     "xxhash_email",
			expr:     "xxhash(s)",
			vars:     map[string]any{"s": "user@example.com"},
			expected: "e92a4a9e39fe3d60",
		},
		{
			name:     "sha256",
			expr:     "sha256(s)",
			vars:     map[string]any{"s": "hello"},
			expected: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
		},
		{
			name:     "sha256_email",
			expr:     "sha256(s)",
			vars:     map[string]any{"s": "user@example.com"},
			expected: "b4c9a289323b21a01c3e940f150eb9b8c542587f1abfd8f0e1cc1ffc5e475514",
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
				t.Errorf("got %v, want %v", out.Value(), tc.expected)
			}
		})
	}
}

func TestCryptoFuncs_OutputLength(t *testing.T) {
	env, err := cel.NewEnv(
		CryptoFuncs(),
		cel.Variable("s", cel.StringType),
	)
	if err != nil {
		t.Fatalf("NewEnv: %v", err)
	}

	ast, _ := env.Compile("xxhash(s)")
	prog, _ := env.Program(ast)
	out, _, _ := prog.Eval(map[string]any{"s": "test"})
	xxhashLen := len(out.Value().(string))
	if xxhashLen > 16 {
		t.Errorf("xxhash output too long: got %d chars, want <= 16", xxhashLen)
	}
	
	ast2, _ := env.Compile("sha256(s)")
	prog2, _ := env.Program(ast2)
	out2, _, _ := prog2.Eval(map[string]any{"s": "test"})
	sha256Len := len(out2.Value().(string))
	if sha256Len != 64 {
		t.Errorf("sha256 output length: got %d chars, want 64", sha256Len)
	}
}
