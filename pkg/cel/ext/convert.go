package ext

import (
	"fmt"
	"strconv"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// ConvertFuncs returns CEL environment options for type conversion functions.
//
// Functions:
//   - toInt(string) -> int: Parse string to int
//   - toInt(double) -> int: Truncate double to int
//   - toInt(bool) -> int: Convert bool to 0/1
//   - toDouble(string) -> double: Parse string to double
//   - toDouble(int) -> double: Convert int to double
//   - toString(int) -> string: Format int as string
//   - toString(double) -> string: Format double as string
//   - toString(bool) -> string: Format bool as "true"/"false"
//   - toBool(string) -> bool: Parse "true"/"false" to bool
//   - toBool(int) -> bool: 0 = false, non-zero = true
func ConvertFuncs() cel.EnvOption {
	return cel.Lib(&convertLib{})
}

type convertLib struct{}

func (l *convertLib) LibraryName() string {
	return "unijord.convert"
}

func (l *convertLib) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function("toInt",
			cel.Overload("toInt_string",
				[]*cel.Type{cel.StringType},
				cel.IntType,
				cel.UnaryBinding(func(s ref.Val) ref.Val {
					str := string(s.(types.String))
					i, err := strconv.ParseInt(str, 10, 64)
					if err != nil {
						return types.NewErr("toInt: %s", err)
					}
					return types.Int(i)
				}),
			),
			cel.Overload("toInt_double",
				[]*cel.Type{cel.DoubleType},
				cel.IntType,
				cel.UnaryBinding(func(d ref.Val) ref.Val {
					return types.Int(int64(d.(types.Double)))
				}),
			),
			cel.Overload("toInt_bool",
				[]*cel.Type{cel.BoolType},
				cel.IntType,
				cel.UnaryBinding(func(b ref.Val) ref.Val {
					if b.(types.Bool) {
						return types.Int(1)
					}
					return types.Int(0)
				}),
			),
		),
		cel.Function("toDouble",
			cel.Overload("toDouble_string",
				[]*cel.Type{cel.StringType},
				cel.DoubleType,
				cel.UnaryBinding(func(s ref.Val) ref.Val {
					str := string(s.(types.String))
					f, err := strconv.ParseFloat(str, 64)
					if err != nil {
						return types.NewErr("toDouble: %s", err)
					}
					return types.Double(f)
				}),
			),
			cel.Overload("toDouble_int",
				[]*cel.Type{cel.IntType},
				cel.DoubleType,
				cel.UnaryBinding(func(i ref.Val) ref.Val {
					return types.Double(i.(types.Int))
				}),
			),
		),
		cel.Function("toString",
			cel.Overload("toString_int",
				[]*cel.Type{cel.IntType},
				cel.StringType,
				cel.UnaryBinding(func(i ref.Val) ref.Val {
					return types.String(strconv.FormatInt(int64(i.(types.Int)), 10))
				}),
			),
			cel.Overload("toString_double",
				[]*cel.Type{cel.DoubleType},
				cel.StringType,
				cel.UnaryBinding(func(d ref.Val) ref.Val {
					return types.String(strconv.FormatFloat(float64(d.(types.Double)), 'f', -1, 64))
				}),
			),
			cel.Overload("toString_bool",
				[]*cel.Type{cel.BoolType},
				cel.StringType,
				cel.UnaryBinding(func(b ref.Val) ref.Val {
					return types.String(fmt.Sprintf("%t", bool(b.(types.Bool))))
				}),
			),
		),
		cel.Function("toBool",
			cel.Overload("toBool_string",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.UnaryBinding(func(s ref.Val) ref.Val {
					str := string(s.(types.String))
					b, err := strconv.ParseBool(str)
					if err != nil {
						return types.NewErr("toBool: %s", err)
					}
					return types.Bool(b)
				}),
			),
			cel.Overload("toBool_int",
				[]*cel.Type{cel.IntType},
				cel.BoolType,
				cel.UnaryBinding(func(i ref.Val) ref.Val {
					return types.Bool(i.(types.Int) != 0)
				}),
			),
		),
	}
}

func (l *convertLib) ProgramOptions() []cel.ProgramOption {
	return nil
}
