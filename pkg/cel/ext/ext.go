// Package ext provides custom CEL function extensions for the transform pipeline.
//
// # Temporal Functions (TemporalFuncs)
//
// Functions for extracting date/time components from timestamps:
//   - year(timestamp) -> int
//   - month(timestamp) -> int
//   - day(timestamp) -> int
//   - hour(timestamp) -> int
//   - minute(timestamp) -> int
//   - second(timestamp) -> int
//   - dayOfWeek(timestamp) -> int
//   - dayOfYear(timestamp) -> int
//   - weekOfYear(timestamp) -> int
//   - date(timestamp) -> string
//   - parseTimestamp(string, layout) -> timestamp
//   - epochMillis(timestamp) -> int
//   - fromEpochMillis(int) -> timestamp
//
// # String Functions (StringFuncs)
//
// Functions for string manipulation:
//   - lower(string) -> string
//   - upper(string) -> string
//   - trim(string) -> string
//   - trimPrefix(string, prefix) -> string
//   - trimSuffix(string, suffix) -> string
//   - replace(string, old, new) -> string
//   - split(string, separator) -> list<string>
//   - join(list<string>, separator) -> string
//   - substr(string, start, length) -> string
//   - reverse(string) -> string
//   - padLeft(string, length, char) -> string
//   - padRight(string, length, char) -> string
//   - regexMatch(string, pattern) -> bool
//   - regexReplace(string, pattern, replacement) -> string
//
// # Crypto Functions (CryptoFuncs)
//
// Functions for hashing:
//   - xxhash(string) -> string (fast, non-cryptographic, 16 hex chars)
//   - sha256(string) -> string (cryptographic, 64 hex chars)
//
// # List Functions (ListFuncs)
//
// Functions for list/array operations:
//   - sum(list<int>) -> int
//   - sumDouble(list<double>) -> double
//   - min(list<int>) -> int
//   - minDouble(list<double>) -> double
//   - max(list<int>) -> int
//   - maxDouble(list<double>) -> double
//   - avg(list<int>) -> double
//   - avgDouble(list<double>) -> double
//   - first(list<dyn>) -> dyn
//   - last(list<dyn>) -> dyn
//   - flatten(list<list<dyn>>) -> list<dyn>
//
// # Convert Functions (ConvertFuncs)
//
// Functions for type conversion:
//   - toInt(string|double|bool) -> int
//   - toDouble(string|int) -> double
//   - toString(int|double|bool) -> string
//   - toBool(string|int) -> bool
//
// # Null Functions (NullFuncs)
//
// Functions for null value handling:
//   - coalesce(dyn, dyn) -> dyn
//   - coalesce3(dyn, dyn, dyn) -> dyn
//   - coalesce4(dyn, dyn, dyn, dyn) -> dyn
//   - ifNull(dyn, dyn) -> dyn
//   - orDefault(dyn, dyn) -> dyn
//   - isNull(dyn) -> bool
//   - isNotNull(dyn) -> bool
//   - nullIf(dyn, dyn) -> dyn
//

package ext

import "github.com/google/cel-go/cel"

// AllFuncs returns all custom CEL function libraries as a slice of EnvOptions.
func AllFuncs() []cel.EnvOption {
	return []cel.EnvOption{
		TemporalFuncs(),
		StringFuncs(),
		CryptoFuncs(),
		ListFuncs(),
		ConvertFuncs(),
		NullFuncs(),
	}
}
