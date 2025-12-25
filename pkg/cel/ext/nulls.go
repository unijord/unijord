package ext

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// NullFuncs returns CEL environment options for null handling functions.
//
// Functions:
//   - coalesce(dyn, dyn, ...) -> dyn: Return first non-null value
//   - ifNull(dyn, dyn) -> dyn: Return first arg if non-null, else second
//   - isNull(dyn) -> bool: Check if value is null
//   - isNotNull(dyn) -> bool: Check if value is not null
//   - nullIf(dyn, dyn) -> dyn: Return null if first equals second, else first
//   - orDefault(dyn, dyn) -> dyn: Alias for ifNull
func NullFuncs() cel.EnvOption {
	return cel.Lib(&nullLib{})
}

type nullLib struct{}

func (l *nullLib) LibraryName() string {
	return "unijord.nulls"
}

func (l *nullLib) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		// coalesce with 2 arguments
		cel.Function("coalesce",
			cel.Overload("coalesce_dyn_dyn",
				[]*cel.Type{cel.DynType, cel.DynType},
				cel.DynType,
				cel.BinaryBinding(func(a, b ref.Val) ref.Val {
					if a.Type() != types.NullType {
						return a
					}
					return b
				}),
			),
		),
		// coalesce with 3 arguments
		cel.Function("coalesce3",
			cel.Overload("coalesce3_dyn_dyn_dyn",
				[]*cel.Type{cel.DynType, cel.DynType, cel.DynType},
				cel.DynType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					for _, arg := range args {
						if arg.Type() != types.NullType {
							return arg
						}
					}
					return types.NullValue
				}),
			),
		),
		// coalesce with 4 arguments
		cel.Function("coalesce4",
			cel.Overload("coalesce4_dyn_dyn_dyn_dyn",
				[]*cel.Type{cel.DynType, cel.DynType, cel.DynType, cel.DynType},
				cel.DynType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					for _, arg := range args {
						if arg.Type() != types.NullType {
							return arg
						}
					}
					return types.NullValue
				}),
			),
		),
		cel.Function("ifNull",
			cel.Overload("ifNull_dyn_dyn",
				[]*cel.Type{cel.DynType, cel.DynType},
				cel.DynType,
				cel.BinaryBinding(func(value, defaultVal ref.Val) ref.Val {
					if value.Type() != types.NullType {
						return value
					}
					return defaultVal
				}),
			),
		),
		cel.Function("orDefault",
			cel.Overload("orDefault_dyn_dyn",
				[]*cel.Type{cel.DynType, cel.DynType},
				cel.DynType,
				cel.BinaryBinding(func(value, defaultVal ref.Val) ref.Val {
					if value.Type() != types.NullType {
						return value
					}
					return defaultVal
				}),
			),
		),
		cel.Function("isNull",
			cel.Overload("isNull_dyn",
				[]*cel.Type{cel.DynType},
				cel.BoolType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					return types.Bool(value.Type() == types.NullType)
				}),
			),
		),
		cel.Function("isNotNull",
			cel.Overload("isNotNull_dyn",
				[]*cel.Type{cel.DynType},
				cel.BoolType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					return types.Bool(value.Type() != types.NullType)
				}),
			),
		),
		cel.Function("nullIf",
			cel.Overload("nullIf_dyn_dyn",
				[]*cel.Type{cel.DynType, cel.DynType},
				cel.DynType,
				cel.BinaryBinding(func(value, check ref.Val) ref.Val {
					if value.Equal(check) == types.True {
						return types.NullValue
					}
					return value
				}),
			),
		),
	}
}

func (l *nullLib) ProgramOptions() []cel.ProgramOption {
	return nil
}
