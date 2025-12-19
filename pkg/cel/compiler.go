package cel

import (
	"fmt"

	"github.com/google/cel-go/cel"
)

type Compiler struct {
	env *cel.Env
}

func NewCompiler(env *cel.Env) *Compiler {
	return &Compiler{env: env}
}

func (c *Compiler) Compile(expr string) (*CompiledExpr, error) {
	ast, issues := c.env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("compile: %w", issues.Err())
	}

	prog, err := c.env.Program(ast, cel.EvalOptions(cel.OptOptimize))
	if err != nil {
		return nil, fmt.Errorf("program: %w", err)
	}

	return &CompiledExpr{
		source:     expr,
		program:    prog,
		outputType: exprTypeFromCEL(ast.OutputType()),
	}, nil
}

func (c *Compiler) CompileBool(expr string) (*CompiledExpr, error) {
	compiled, err := c.Compile(expr)
	if err != nil {
		return nil, err
	}
	if compiled.outputType != ExprTypeBool {
		return nil, fmt.Errorf("expression must return bool, got %s", compiled.outputType)
	}
	return compiled, nil
}

func (c *Compiler) CompileString(expr string) (*CompiledExpr, error) {
	compiled, err := c.Compile(expr)
	if err != nil {
		return nil, err
	}
	if compiled.outputType != ExprTypeString {
		return nil, fmt.Errorf("expression must return string, got %s", compiled.outputType)
	}
	return compiled, nil
}

func (c *Compiler) CompileInt(expr string) (*CompiledExpr, error) {
	compiled, err := c.Compile(expr)
	if err != nil {
		return nil, err
	}
	if compiled.outputType != ExprTypeInt {
		return nil, fmt.Errorf("expression must return int, got %s", compiled.outputType)
	}
	return compiled, nil
}
