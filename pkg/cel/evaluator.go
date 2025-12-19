package cel

import (
	"fmt"
)

type Evaluator struct{}

func NewEvaluator() *Evaluator {
	return &Evaluator{}
}

func (e *Evaluator) Eval(expr *CompiledExpr, vars map[string]any) EvalResult {
	out, _, err := expr.program.Eval(vars)
	if err != nil {
		return NewEvalError(fmt.Errorf("eval: %w", err))
	}
	return NewEvalResult(out.Value(), expr.outputType)
}

func (e *Evaluator) EvalBool(expr *CompiledExpr, vars map[string]any) (bool, error) {
	result := e.Eval(expr, vars)
	return result.Bool()
}

func (e *Evaluator) EvalString(expr *CompiledExpr, vars map[string]any) (string, error) {
	result := e.Eval(expr, vars)
	return result.String()
}

func (e *Evaluator) EvalInt(expr *CompiledExpr, vars map[string]any) (int64, error) {
	result := e.Eval(expr, vars)
	return result.Int()
}

func (e *Evaluator) EvalDouble(expr *CompiledExpr, vars map[string]any) (float64, error) {
	result := e.Eval(expr, vars)
	return result.Double()
}

func (e *Evaluator) EvalAny(expr *CompiledExpr, vars map[string]any) (any, error) {
	result := e.Eval(expr, vars)
	if result.Err() != nil {
		return nil, result.Err()
	}
	return result.Value(), nil
}
