package ext

import (
	"regexp"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// StringFuncs returns CEL environment options for string manipulation functions.
//
// Functions:
//   - lower(string) -> string: Convert to lowercase
//   - upper(string) -> string: Convert to uppercase
//   - trim(string) -> string: Remove leading/trailing whitespace
//   - trimPrefix(string, prefix) -> string: Remove prefix if present
//   - trimSuffix(string, suffix) -> string: Remove suffix if present
//   - replace(string, old, new) -> string: Replace all occurrences
//   - split(string, separator) -> list<string>: Split string by separator
//   - join(list<string>, separator) -> string: Join strings with separator
//   - substr(string, start, length) -> string: Extract substring
//   - padLeft(string, length, char) -> string: Pad string on the left
//   - padRight(string, length, char) -> string: Pad string on the right
//   - regexMatch(string, pattern) -> bool: Check if string matches regex
//   - regexReplace(string, pattern, replacement) -> string: Replace regex matches
//   - mask(string, showFirst, showLast) -> string: Mask middle characters with *
//   - truncate(string, maxLen) -> string: Truncate with ellipsis if too long
//   - extractDomain(email) -> string: Extract domain from email address
func StringFuncs() cel.EnvOption {
	return cel.Lib(&stringLib{})
}

type stringLib struct{}

func (l *stringLib) LibraryName() string {
	return "unijord.strings"
}

func (l *stringLib) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function("lower",
			cel.Overload("lower_string",
				[]*cel.Type{cel.StringType},
				cel.StringType,
				cel.UnaryBinding(func(s ref.Val) ref.Val {
					return types.String(strings.ToLower(string(s.(types.String))))
				}),
			),
		),
		cel.Function("upper",
			cel.Overload("upper_string",
				[]*cel.Type{cel.StringType},
				cel.StringType,
				cel.UnaryBinding(func(s ref.Val) ref.Val {
					return types.String(strings.ToUpper(string(s.(types.String))))
				}),
			),
		),
		cel.Function("trim",
			cel.Overload("trim_string",
				[]*cel.Type{cel.StringType},
				cel.StringType,
				cel.UnaryBinding(func(s ref.Val) ref.Val {
					return types.String(strings.TrimSpace(string(s.(types.String))))
				}),
			),
		),
		cel.Function("trimPrefix",
			cel.Overload("trimPrefix_string_string",
				[]*cel.Type{cel.StringType, cel.StringType},
				cel.StringType,
				cel.BinaryBinding(func(s, prefix ref.Val) ref.Val {
					return types.String(strings.TrimPrefix(
						string(s.(types.String)),
						string(prefix.(types.String)),
					))
				}),
			),
		),
		cel.Function("trimSuffix",
			cel.Overload("trimSuffix_string_string",
				[]*cel.Type{cel.StringType, cel.StringType},
				cel.StringType,
				cel.BinaryBinding(func(s, suffix ref.Val) ref.Val {
					return types.String(strings.TrimSuffix(
						string(s.(types.String)),
						string(suffix.(types.String)),
					))
				}),
			),
		),
		cel.Function("replace",
			cel.Overload("replace_string_string_string",
				[]*cel.Type{cel.StringType, cel.StringType, cel.StringType},
				cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					s := string(args[0].(types.String))
					old := string(args[1].(types.String))
					n := string(args[2].(types.String))
					return types.String(strings.ReplaceAll(s, old, n))
				}),
			),
		),
		cel.Function("split",
			cel.Overload("split_string_string",
				[]*cel.Type{cel.StringType, cel.StringType},
				cel.ListType(cel.StringType),
				cel.BinaryBinding(func(s, sep ref.Val) ref.Val {
					parts := strings.Split(
						string(s.(types.String)),
						string(sep.(types.String)),
					)
					return types.DefaultTypeAdapter.NativeToValue(parts)
				}),
			),
		),
		cel.Function("join",
			cel.Overload("join_list_string",
				[]*cel.Type{cel.ListType(cel.StringType), cel.StringType},
				cel.StringType,
				cel.BinaryBinding(func(list, sep ref.Val) ref.Val {
					l := list.(traits.Lister)
					size := l.Size().(types.Int)
					parts := make([]string, int(size))
					for i := 0; i < int(size); i++ {
						parts[i] = string(l.Get(types.Int(i)).(types.String))
					}
					return types.String(strings.Join(parts, string(sep.(types.String))))
				}),
			),
		),
		cel.Function("substr",
			cel.Overload("substr_string_int_int",
				[]*cel.Type{cel.StringType, cel.IntType, cel.IntType},
				cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					s := string(args[0].(types.String))
					start := int(args[1].(types.Int))
					length := int(args[2].(types.Int))

					runes := []rune(s)
					if start < 0 {
						start = 0
					}
					if start >= len(runes) {
						return types.String("")
					}
					end := start + length
					if end > len(runes) {
						end = len(runes)
					}
					return types.String(runes[start:end])
				}),
			),
		),
		cel.Function("padLeft",
			cel.Overload("padLeft_string_int_string",
				[]*cel.Type{cel.StringType, cel.IntType, cel.StringType},
				cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					s := string(args[0].(types.String))
					length := int(args[1].(types.Int))
					pad := string(args[2].(types.String))

					if len(pad) == 0 {
						return types.String(s)
					}
					for len(s) < length {
						s = pad + s
					}

					if len(s) > length {
						s = s[len(s)-length:]
					}
					return types.String(s)
				}),
			),
		),
		cel.Function("padRight",
			cel.Overload("padRight_string_int_string",
				[]*cel.Type{cel.StringType, cel.IntType, cel.StringType},
				cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					s := string(args[0].(types.String))
					length := int(args[1].(types.Int))
					pad := string(args[2].(types.String))

					if len(pad) == 0 {
						return types.String(s)
					}
					for len(s) < length {
						s = s + pad
					}

					if len(s) > length {
						s = s[:length]
					}
					return types.String(s)
				}),
			),
		),
		cel.Function("regexMatch",
			cel.Overload("regexMatch_string_string",
				[]*cel.Type{cel.StringType, cel.StringType},
				cel.BoolType,
				cel.BinaryBinding(func(s, pattern ref.Val) ref.Val {
					re, err := regexp.Compile(string(pattern.(types.String)))
					if err != nil {
						return types.NewErr("regexMatch: invalid pattern: %s", err)
					}
					return types.Bool(re.MatchString(string(s.(types.String))))
				}),
			),
		),
		cel.Function("regexReplace",
			cel.Overload("regexReplace_string_string_string",
				[]*cel.Type{cel.StringType, cel.StringType, cel.StringType},
				cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					s := string(args[0].(types.String))
					pattern := string(args[1].(types.String))
					replacement := string(args[2].(types.String))

					re, err := regexp.Compile(pattern)
					if err != nil {
						return types.NewErr("regexReplace: invalid pattern: %s", err)
					}
					return types.String(re.ReplaceAllString(s, replacement))
				}),
			),
		),
		// mask - mask middle characters, show first N and last M
		cel.Function("mask",
			cel.Overload("mask_string_int_int",
				[]*cel.Type{cel.StringType, cel.IntType, cel.IntType},
				cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					s := string(args[0].(types.String))
					showFirst := int(args[1].(types.Int))
					showLast := int(args[2].(types.Int))

					runes := []rune(s)
					length := len(runes)

					if showFirst < 0 {
						showFirst = 0
					}
					if showLast < 0 {
						showLast = 0
					}

					// showing more than we have, return as-is
					if showFirst+showLast >= length {
						return types.String(s)
					}

					// masked string
					result := make([]rune, length)
					for i := 0; i < length; i++ {
						if i < showFirst || i >= length-showLast {
							result[i] = runes[i]
						} else {
							result[i] = '*'
						}
					}
					return types.String(result)
				}),
			),
		),
		// truncate - truncate string with ellipsis
		cel.Function("truncate",
			cel.Overload("truncate_string_int",
				[]*cel.Type{cel.StringType, cel.IntType},
				cel.StringType,
				cel.BinaryBinding(func(s, maxLen ref.Val) ref.Val {
					str := string(s.(types.String))
					max := int(maxLen.(types.Int))

					if max <= 0 {
						return types.String("")
					}

					runes := []rune(str)
					if len(runes) <= max {
						return types.String(str)
					}

					if max <= 3 {
						return types.String(runes[:max])
					}
					return types.String(string(runes[:max-3]) + "...")
				}),
			),
		),
		// extractDomain - extract domain from email
		cel.Function("extractDomain",
			cel.Overload("extractDomain_string",
				[]*cel.Type{cel.StringType},
				cel.StringType,
				cel.UnaryBinding(func(s ref.Val) ref.Val {
					email := string(s.(types.String))
					idx := strings.LastIndex(email, "@")
					if idx == -1 || idx == len(email)-1 {
						return types.String("")
					}
					return types.String(email[idx+1:])
				}),
			),
		),
	}
}

func (l *stringLib) ProgramOptions() []cel.ProgramOption {
	return nil
}
