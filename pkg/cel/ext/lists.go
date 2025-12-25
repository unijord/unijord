package ext

import (
	"fmt"
	"math"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// ListFuncs returns CEL environment options for list/array operations.
//
// Functions:
//   - sum(list<int>) -> int: Sum of integers
//   - sum(list<double>) -> double: Sum of doubles
//   - min(list<int>) -> int: Minimum integer
//   - min(list<double>) -> double: Minimum double
//   - max(list<int>) -> int: Maximum integer
//   - max(list<double>) -> double: Maximum double
//   - first(list<dyn>) -> dyn: First element (null if empty)
//   - last(list<dyn>) -> dyn: Last element (null if empty)
//   - avg(list<int>) -> double: Average of integers
//   - avg(list<double>) -> double: Average of doubles
//   - flatten(list<list<dyn>>) -> list<dyn>: Flatten nested list
//   - sumBy(list<dyn>, string) -> double: Sum field values from list of maps
//   - avgBy(list<dyn>, string) -> double: Average field values from list of maps
//   - minBy(list<dyn>, string) -> double: Min field value from list of maps
//   - maxBy(list<dyn>, string) -> double: Max field value from list of maps
func ListFuncs() cel.EnvOption {
	return cel.Lib(&listLib{})
}

type listLib struct{}

func (l *listLib) LibraryName() string {
	return "unijord.lists"
}

func (l *listLib) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		// sum for int list
		cel.Function("sum",
			cel.Overload("sum_list_int",
				[]*cel.Type{cel.ListType(cel.IntType)},
				cel.IntType,
				cel.UnaryBinding(func(list ref.Val) ref.Val {
					lister := list.(traits.Lister)
					size := int(lister.Size().(types.Int))
					var sum int64
					for i := 0; i < size; i++ {
						sum += int64(lister.Get(types.Int(i)).(types.Int))
					}
					return types.Int(sum)
				}),
			),
		),
		// sum for double list
		cel.Function("sumDouble",
			cel.Overload("sum_list_double",
				[]*cel.Type{cel.ListType(cel.DoubleType)},
				cel.DoubleType,
				cel.UnaryBinding(func(list ref.Val) ref.Val {
					lister := list.(traits.Lister)
					size := int(lister.Size().(types.Int))
					var sum float64
					for i := 0; i < size; i++ {
						sum += float64(lister.Get(types.Int(i)).(types.Double))
					}
					return types.Double(sum)
				}),
			),
		),
		// min for int list
		cel.Function("min",
			cel.Overload("min_list_int",
				[]*cel.Type{cel.ListType(cel.IntType)},
				cel.IntType,
				cel.UnaryBinding(func(list ref.Val) ref.Val {
					lister := list.(traits.Lister)
					size := int(lister.Size().(types.Int))
					if size == 0 {
						return types.NewErr("min: empty list")
					}
					minVal := int64(lister.Get(types.Int(0)).(types.Int))
					for i := 1; i < size; i++ {
						v := int64(lister.Get(types.Int(i)).(types.Int))
						if v < minVal {
							minVal = v
						}
					}
					return types.Int(minVal)
				}),
			),
		),
		// min for double list
		cel.Function("minDouble",
			cel.Overload("min_list_double",
				[]*cel.Type{cel.ListType(cel.DoubleType)},
				cel.DoubleType,
				cel.UnaryBinding(func(list ref.Val) ref.Val {
					lister := list.(traits.Lister)
					size := int(lister.Size().(types.Int))
					if size == 0 {
						return types.NewErr("minDouble: empty list")
					}
					minVal := float64(lister.Get(types.Int(0)).(types.Double))
					for i := 1; i < size; i++ {
						v := float64(lister.Get(types.Int(i)).(types.Double))
						if v < minVal {
							minVal = v
						}
					}
					return types.Double(minVal)
				}),
			),
		),
		// max for int list
		cel.Function("max",
			cel.Overload("max_list_int",
				[]*cel.Type{cel.ListType(cel.IntType)},
				cel.IntType,
				cel.UnaryBinding(func(list ref.Val) ref.Val {
					lister := list.(traits.Lister)
					size := int(lister.Size().(types.Int))
					if size == 0 {
						return types.NewErr("max: empty list")
					}
					maxVal := int64(lister.Get(types.Int(0)).(types.Int))
					for i := 1; i < size; i++ {
						v := int64(lister.Get(types.Int(i)).(types.Int))
						if v > maxVal {
							maxVal = v
						}
					}
					return types.Int(maxVal)
				}),
			),
		),
		// max for double list
		cel.Function("maxDouble",
			cel.Overload("max_list_double",
				[]*cel.Type{cel.ListType(cel.DoubleType)},
				cel.DoubleType,
				cel.UnaryBinding(func(list ref.Val) ref.Val {
					lister := list.(traits.Lister)
					size := int(lister.Size().(types.Int))
					if size == 0 {
						return types.NewErr("maxDouble: empty list")
					}
					maxVal := float64(lister.Get(types.Int(0)).(types.Double))
					for i := 1; i < size; i++ {
						v := float64(lister.Get(types.Int(i)).(types.Double))
						if v > maxVal {
							maxVal = v
						}
					}
					return types.Double(maxVal)
				}),
			),
		),
		// first element
		cel.Function("first",
			cel.Overload("first_list",
				[]*cel.Type{cel.ListType(cel.DynType)},
				cel.DynType,
				cel.UnaryBinding(func(list ref.Val) ref.Val {
					lister := list.(traits.Lister)
					size := int(lister.Size().(types.Int))
					if size == 0 {
						return types.NullValue
					}
					return lister.Get(types.Int(0))
				}),
			),
		),
		// last element
		cel.Function("last",
			cel.Overload("last_list",
				[]*cel.Type{cel.ListType(cel.DynType)},
				cel.DynType,
				cel.UnaryBinding(func(list ref.Val) ref.Val {
					lister := list.(traits.Lister)
					size := int(lister.Size().(types.Int))
					if size == 0 {
						return types.NullValue
					}
					return lister.Get(types.Int(size - 1))
				}),
			),
		),
		// avg for int list (returns double)
		cel.Function("avg",
			cel.Overload("avg_list_int",
				[]*cel.Type{cel.ListType(cel.IntType)},
				cel.DoubleType,
				cel.UnaryBinding(func(list ref.Val) ref.Val {
					lister := list.(traits.Lister)
					size := int(lister.Size().(types.Int))
					if size == 0 {
						return types.Double(math.NaN())
					}
					var sum int64
					for i := 0; i < size; i++ {
						sum += int64(lister.Get(types.Int(i)).(types.Int))
					}
					return types.Double(float64(sum) / float64(size))
				}),
			),
		),
		// avg for double list
		cel.Function("avgDouble",
			cel.Overload("avg_list_double",
				[]*cel.Type{cel.ListType(cel.DoubleType)},
				cel.DoubleType,
				cel.UnaryBinding(func(list ref.Val) ref.Val {
					lister := list.(traits.Lister)
					size := int(lister.Size().(types.Int))
					if size == 0 {
						return types.Double(math.NaN())
					}
					var sum float64
					for i := 0; i < size; i++ {
						sum += float64(lister.Get(types.Int(i)).(types.Double))
					}
					return types.Double(sum / float64(size))
				}),
			),
		),
		// flatten nested list
		cel.Function("flatten",
			cel.Overload("flatten_list_list",
				[]*cel.Type{cel.ListType(cel.ListType(cel.DynType))},
				cel.ListType(cel.DynType),
				cel.UnaryBinding(func(list ref.Val) ref.Val {
					outer := list.(traits.Lister)
					outerSize := int(outer.Size().(types.Int))
					var result []ref.Val
					for i := 0; i < outerSize; i++ {
						inner := outer.Get(types.Int(i)).(traits.Lister)
						innerSize := int(inner.Size().(types.Int))
						for j := 0; j < innerSize; j++ {
							result = append(result, inner.Get(types.Int(j)))
						}
					}
					return types.DefaultTypeAdapter.NativeToValue(result)
				}),
			),
		),
		// sumBy - sum a field from list of maps
		cel.Function("sumBy",
			cel.Overload("sumBy_list_string",
				[]*cel.Type{cel.ListType(cel.DynType), cel.StringType},
				cel.DoubleType,
				cel.BinaryBinding(func(list, field ref.Val) ref.Val {
					return aggregateByField(list, field, aggSum)
				}),
			),
		),
		// avgBy - average a field from list of maps
		cel.Function("avgBy",
			cel.Overload("avgBy_list_string",
				[]*cel.Type{cel.ListType(cel.DynType), cel.StringType},
				cel.DoubleType,
				cel.BinaryBinding(func(list, field ref.Val) ref.Val {
					return aggregateByField(list, field, aggAvg)
				}),
			),
		),
		// minBy - min of a field from list of maps
		cel.Function("minBy",
			cel.Overload("minBy_list_string",
				[]*cel.Type{cel.ListType(cel.DynType), cel.StringType},
				cel.DoubleType,
				cel.BinaryBinding(func(list, field ref.Val) ref.Val {
					return aggregateByField(list, field, aggMin)
				}),
			),
		),
		// maxBy - max of a field from list of maps
		cel.Function("maxBy",
			cel.Overload("maxBy_list_string",
				[]*cel.Type{cel.ListType(cel.DynType), cel.StringType},
				cel.DoubleType,
				cel.BinaryBinding(func(list, field ref.Val) ref.Val {
					return aggregateByField(list, field, aggMax)
				}),
			),
		),
	}
}

func (l *listLib) ProgramOptions() []cel.ProgramOption {
	return nil
}

// aggregation type constants
type aggType int

const (
	aggSum aggType = iota
	aggAvg
	aggMin
	aggMax
)

// aggregateByField extracts a numeric field from each map in the list and aggregates.
// Supports nested fields via dot notation: "item.price"
func aggregateByField(list, field ref.Val, agg aggType) ref.Val {
	lister, ok := list.(traits.Lister)
	if !ok {
		return types.NewErr("sumBy: first argument must be a list")
	}

	fieldName := string(field.(types.String))
	size := int(lister.Size().(types.Int))

	if size == 0 {
		if agg == aggAvg {
			return types.Double(math.NaN())
		}
		return types.Double(0)
	}

	var sum float64
	var minVal, maxVal float64
	first := true

	for i := 0; i < size; i++ {
		item := lister.Get(types.Int(i))
		val, err := extractNumericField(item, fieldName)
		if err != nil {
			return types.NewErr("%s: %v", aggName(agg), err)
		}

		sum += val
		if first {
			minVal = val
			maxVal = val
			first = false
		} else {
			if val < minVal {
				minVal = val
			}
			if val > maxVal {
				maxVal = val
			}
		}
	}

	switch agg {
	case aggSum:
		return types.Double(sum)
	case aggAvg:
		return types.Double(sum / float64(size))
	case aggMin:
		return types.Double(minVal)
	case aggMax:
		return types.Double(maxVal)
	default:
		return types.Double(sum)
	}
}

func aggName(agg aggType) string {
	switch agg {
	case aggSum:
		return "sumBy"
	case aggAvg:
		return "avgBy"
	case aggMin:
		return "minBy"
	case aggMax:
		return "maxBy"
	default:
		return "aggregateBy"
	}
}

// extractNumericField gets a numeric value from a map, supporting nested field access.
func extractNumericField(item ref.Val, fieldPath string) (float64, error) {
	// nested paths like "item.price"
	current := item
	for _, part := range splitFieldPath(fieldPath) {
		mapper, ok := current.(traits.Mapper)
		if !ok {
			return 0, fmt.Errorf("cannot access field %q on non-map value", part)
		}
		current = mapper.Get(types.String(part))
		if types.IsError(current) {
			return 0, fmt.Errorf("field %q not found", part)
		}
	}

	// to float64
	switch v := current.(type) {
	case types.Int:
		return float64(v), nil
	case types.Double:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("field value is not numeric: %T", current)
	}
}

// splitFieldPath splits "a.b.c" into ["a", "b", "c"]
func splitFieldPath(path string) []string {
	if path == "" {
		return nil
	}
	var parts []string
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			if i > start {
				parts = append(parts, path[start:i])
			}
			start = i + 1
		}
	}
	if start < len(path) {
		parts = append(parts, path[start:])
	}
	return parts
}
