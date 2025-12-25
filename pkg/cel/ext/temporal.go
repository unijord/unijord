package ext

import (
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// TemporalFuncs returns CEL environment options for temporal functions.
// These functions extract date/time components from timestamps.
//
// Functions:
//   - year(timestamp) -> int: Extract year (e.g., 2024)
//   - month(timestamp) -> int: Extract month (1-12)
//   - day(timestamp) -> int: Extract day of month (1-31)
//   - hour(timestamp) -> int: Extract hour (0-23)
//   - minute(timestamp) -> int: Extract minute (0-59)
//   - second(timestamp) -> int: Extract second (0-59)
//   - dayOfWeek(timestamp) -> int: Extract day of week (0=Sunday, 6=Saturday)
//   - dayOfYear(timestamp) -> int: Extract day of year (1-366)
//   - weekOfYear(timestamp) -> int: Extract ISO week number (1-53)
//   - date(timestamp) -> string: Format as "2006-01-02"
//   - parseTimestamp(string, layout) -> timestamp: Parse string to timestamp
//   - epochMillis(timestamp) -> int: Convert timestamp to epoch milliseconds
//   - fromEpochMillis(int) -> timestamp: Convert epoch milliseconds to timestamp
func TemporalFuncs() cel.EnvOption {
	return cel.Lib(&temporalLib{})
}

type temporalLib struct{}

func (l *temporalLib) LibraryName() string {
	return "unijord.temporal"
}

func (l *temporalLib) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function("year",
			cel.Overload("year_timestamp",
				[]*cel.Type{cel.TimestampType},
				cel.IntType,
				cel.UnaryBinding(func(ts ref.Val) ref.Val {
					t := ts.Value().(time.Time)
					return types.Int(t.Year())
				}),
			),
		),
		cel.Function("month",
			cel.Overload("month_timestamp",
				[]*cel.Type{cel.TimestampType},
				cel.IntType,
				cel.UnaryBinding(func(ts ref.Val) ref.Val {
					t := ts.Value().(time.Time)
					return types.Int(t.Month())
				}),
			),
		),
		cel.Function("day",
			cel.Overload("day_timestamp",
				[]*cel.Type{cel.TimestampType},
				cel.IntType,
				cel.UnaryBinding(func(ts ref.Val) ref.Val {
					t := ts.Value().(time.Time)
					return types.Int(t.Day())
				}),
			),
		),
		cel.Function("hour",
			cel.Overload("hour_timestamp",
				[]*cel.Type{cel.TimestampType},
				cel.IntType,
				cel.UnaryBinding(func(ts ref.Val) ref.Val {
					t := ts.Value().(time.Time)
					return types.Int(t.Hour())
				}),
			),
		),
		cel.Function("minute",
			cel.Overload("minute_timestamp",
				[]*cel.Type{cel.TimestampType},
				cel.IntType,
				cel.UnaryBinding(func(ts ref.Val) ref.Val {
					t := ts.Value().(time.Time)
					return types.Int(t.Minute())
				}),
			),
		),
		cel.Function("second",
			cel.Overload("second_timestamp",
				[]*cel.Type{cel.TimestampType},
				cel.IntType,
				cel.UnaryBinding(func(ts ref.Val) ref.Val {
					t := ts.Value().(time.Time)
					return types.Int(t.Second())
				}),
			),
		),
		cel.Function("dayOfWeek",
			cel.Overload("dayOfWeek_timestamp",
				[]*cel.Type{cel.TimestampType},
				cel.IntType,
				cel.UnaryBinding(func(ts ref.Val) ref.Val {
					t := ts.Value().(time.Time)
					return types.Int(t.Weekday())
				}),
			),
		),
		cel.Function("dayOfYear",
			cel.Overload("dayOfYear_timestamp",
				[]*cel.Type{cel.TimestampType},
				cel.IntType,
				cel.UnaryBinding(func(ts ref.Val) ref.Val {
					t := ts.Value().(time.Time)
					return types.Int(t.YearDay())
				}),
			),
		),
		cel.Function("weekOfYear",
			cel.Overload("weekOfYear_timestamp",
				[]*cel.Type{cel.TimestampType},
				cel.IntType,
				cel.UnaryBinding(func(ts ref.Val) ref.Val {
					t := ts.Value().(time.Time)
					_, week := t.ISOWeek()
					return types.Int(week)
				}),
			),
		),
		cel.Function("date",
			cel.Overload("date_timestamp",
				[]*cel.Type{cel.TimestampType},
				cel.StringType,
				cel.UnaryBinding(func(ts ref.Val) ref.Val {
					t := ts.Value().(time.Time)
					return types.String(t.Format("2006-01-02"))
				}),
			),
		),
		cel.Function("parseTimestamp",
			cel.Overload("parseTimestamp_string_string",
				[]*cel.Type{cel.StringType, cel.StringType},
				cel.TimestampType,
				cel.BinaryBinding(func(s, layout ref.Val) ref.Val {
					str := string(s.(types.String))
					l := string(layout.(types.String))
					t, err := time.Parse(l, str)
					if err != nil {
						return types.NewErr("parseTimestamp: %s", err)
					}
					return types.Timestamp{Time: t}
				}),
			),
		),
		cel.Function("epochMillis",
			cel.Overload("epochMillis_timestamp",
				[]*cel.Type{cel.TimestampType},
				cel.IntType,
				cel.UnaryBinding(func(ts ref.Val) ref.Val {
					t := ts.Value().(time.Time)
					return types.Int(t.UnixMilli())
				}),
			),
		),
		cel.Function("fromEpochMillis",
			cel.Overload("fromEpochMillis_int",
				[]*cel.Type{cel.IntType},
				cel.TimestampType,
				cel.UnaryBinding(func(ms ref.Val) ref.Val {
					millis := int64(ms.(types.Int))
					return types.Timestamp{Time: time.UnixMilli(millis)}
				}),
			),
		),
	}
}

func (l *temporalLib) ProgramOptions() []cel.ProgramOption {
	return nil
}
