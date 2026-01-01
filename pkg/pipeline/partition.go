package pipeline

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/twmb/murmur3"
)

// Iceberg partition transforms produce two outputs:
// 1. Human-readable string for Hive-style directory paths (e.g., "dt=2024-12-19")
// 2. Iceberg internal value for manifest metadata (e.g., days since epoch: 20077)
// Manifest files store partition bounds for query optimization:
//
//  {
//    "data_file": "dt=2024-12-19/file.parquet",
//    "partition": {
//      "dt": 20077  <- Integer, not string
//    }
//  }
// Reference: https://iceberg.apache.org/spec/#partition-transforms

// PartitionTransform defines how to transform a source column into a partition value.
type PartitionTransform string

// Taken from ICEBERG, Partition - https://iceberg.apache.org/spec/#partition-transforms
// Use value as-is - TransformIdentity
// Hash into N buckets - TransformBucket
// Truncate to width W - TransformTruncate
// For example: A configuration like this
//
//	{
//	   "partition_spec": [
//	     {"source_id": 1, "transform": "day", "name": "dt"},
//	     {"source_id": 2, "transform": "bucket", "param": 16, "name": "user_bucket"}
//	   ]
//	 }
//
// Results in paths like:
//
//	data/dt=2024-12-31/user_bucket=7/file.parquet
const (
	TransformIdentity PartitionTransform = "identity"
	TransformBucket   PartitionTransform = "bucket"
	TransformTruncate PartitionTransform = "truncate"
	TransformYear     PartitionTransform = "year"
	TransformMonth    PartitionTransform = "month"
	TransformDay      PartitionTransform = "day"
	TransformHour     PartitionTransform = "hour"
	TransformVoid     PartitionTransform = "void"
)

// TransformResult contains both human-readable and Iceberg-internal partition values.
type TransformResult struct {
	// PathValue is the human-readable value for Hive-style directory paths.
	// Example: "2024-12-19" for day transform, "us-west" for identity.
	PathValue string

	// IcebergValue is the internal value for Iceberg manifest metadata.
	// For temporal transforms, this is an integer (days/months/years/hours since epoch).
	// For identity/truncate, this is the same as PathValue.
	// For bucket, this is the bucket number (0 to N-1).
	IcebergValue any
}

// ApplyTransform applies a partition transform to a value.
// Returns both human-readable path value and Iceberg internal value.
func ApplyTransform(value any, transform PartitionTransform, param int) TransformResult {
	switch transform {
	case TransformIdentity:
		return applyIdentity(value)
	case TransformBucket:
		return applyBucket(value, param)
	case TransformTruncate:
		return applyTruncate(value, param)
	case TransformYear:
		return applyYear(value)
	case TransformMonth:
		return applyMonth(value)
	case TransformDay:
		return applyDay(value)
	case TransformHour:
		return applyHour(value)
	case TransformVoid:
		return TransformResult{PathValue: "", IcebergValue: nil}
	default:
		// Unknown transform, use identity
		return applyIdentity(value)
	}
}

// applyIdentity returns the value as-is.
func applyIdentity(value any) TransformResult {
	str := fmt.Sprintf("%v", value)
	return TransformResult{
		PathValue:    str,
		IcebergValue: value,
	}
}

// applyBucket hashes the value and returns bucket number (0 to N-1).
// Uses Murmur3 x86 32-bit hash with seed 0 per Iceberg spec.
// Reference: https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
// def bucket_N(x) = (murmur3_x86_32_hash(x) & Integer.MAX_VALUE) % N
func applyBucket(value any, numBuckets int) TransformResult {
	if numBuckets <= 0 {
		numBuckets = 1
	}

	hash := icebergHash(value)

	bucket := int(hash&0x7FFFFFFF) % numBuckets

	return TransformResult{
		PathValue:    fmt.Sprintf("%d", bucket),
		IcebergValue: bucket,
	}
}

// icebergHash computes the Iceberg-compatible 32-bit hash for a value.
// Uses Murmur3 x86 32-bit with seed 0, with type-specific preprocessing.
// Reference: https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
func icebergHash(value any) uint32 {
	switch v := value.(type) {
	case int:
		// int hashes as long to ensure schema evolution compatibility
		return hashLong(int64(v))
	case int32:
		return hashLong(int64(v))
	case int64:
		return hashLong(v)
	case uint32:
		return hashLong(int64(v))
	case uint64:
		return hashLong(int64(v))
	case string:
		return murmur3.StringSum32(v)
	case []byte:
		return murmur3.Sum32(v)
	case time.Time:
		// hash as microseconds from Unix epoch
		return hashLong(v.UnixMicro())
	case float32:
		// hashes as double to ensure schema evolution compatibility
		return hashDouble(float64(v))
	case float64:
		return hashDouble(v)
	case bool:
		if v {
			return hashInt(1)
		}
		return hashInt(0)
	default:
		return murmur3.StringSum32(fmt.Sprintf("%v", value))
	}
}

// hashLong hashes a 64-bit integer using little-endian byte representation.
func hashLong(v int64) uint32 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	return murmur3.Sum32(buf[:])
}

// hashInt hashes a 32-bit integer (used for boolean).
func hashInt(v int32) uint32 {
	return hashLong(int64(v))
}

// doubleToLongBits must give the IEEE 754 compliant bit representation of the double value.
// All NaN bit patterns must be canonicalized to 0x7ff8000000000000L.
// Negative zero (-0.0) must be canonicalized to positive zero (0.0).
// Float hash values are the result of hashing the float cast to
// double to ensure that schema evolution does not change hash values if float types are promoted.

// floorDiv performs floor division, Iceberg truncate uses floor division.
// from iceberg spec.
// The remainder, v % W, must be positive. For languages where % can produce negative values,
// the correct truncate function is: v - (((v % W) + W) % W)
// Example: floorDiv(-999, 100) = -10 in iceberg, but -999/100 = -9 in Go.
func floorDiv(a, b int64) int64 {
	q := a / b
	// signs differ, remainder, adjust toward negative infinity
	if (a^b) < 0 && a%b != 0 {
		q--
	}
	return q
}

// hashDouble hashes a double using IEEE 754 bit representation.
// Per Iceberg spec:
// - NaN patterns canonicalized to 0x7ff8000000000000
// - Negative zero (-0.0) canonicalized to positive zero (0.0)
func hashDouble(v float64) uint32 {
	var bits uint64
	switch {
	case math.IsNaN(v):
		bits = 0x7ff8000000000000
	case v == 0:
		bits = 0
	default:
		bits = math.Float64bits(v)
	}
	return hashLong(int64(bits))
}

// applyTruncate truncates strings to width W (in characters), or integers to bins of width W.
// As Per Iceberg spec, string truncation is character-based, not byte-based.
func applyTruncate(value any, width int) TransformResult {
	if width <= 0 {
		width = 1
	}

	switch v := value.(type) {
	case string:
		// we must truncate by Unicode characters, not bytes
		runes := []rune(v)
		if len(runes) > width {
			v = string(runes[:width])
		}
		return TransformResult{PathValue: v, IcebergValue: v}
	case int:
		w := int64(width)
		truncated := int(floorDiv(int64(v), w) * w)
		return TransformResult{PathValue: fmt.Sprintf("%d", truncated), IcebergValue: truncated}
	case int32:
		w := int64(width)
		truncated := int32(floorDiv(int64(v), w) * w)
		return TransformResult{PathValue: fmt.Sprintf("%d", truncated), IcebergValue: int(truncated)}
	case int64:
		w := int64(width)
		truncated := floorDiv(v, w) * w
		return TransformResult{PathValue: fmt.Sprintf("%d", truncated), IcebergValue: truncated}
	default:
		str := fmt.Sprintf("%v", value)
		runes := []rune(str)
		if len(runes) > width {
			str = string(runes[:width])
		}
		return TransformResult{PathValue: str, IcebergValue: str}
	}
}

// applyYear extracts year from timestamp.
// Iceberg value: years since 1970 (e.g., 2024 → 54)
// Path value: "2024"
func applyYear(value any) TransformResult {
	t := toTime(value)
	if t.IsZero() {
		return TransformResult{PathValue: "", IcebergValue: nil}
	}

	year := t.Year()
	icebergValue := year - 1970

	return TransformResult{
		PathValue:    fmt.Sprintf("%d", year),
		IcebergValue: icebergValue,
	}
}

// applyMonth extracts year-month from timestamp.
// Iceberg value: months since 1970-01 (e.g., 2024-12 → 659)
// Path value: "2024-12"
func applyMonth(value any) TransformResult {
	t := toTime(value)
	if t.IsZero() {
		return TransformResult{PathValue: "", IcebergValue: nil}
	}

	year := t.Year()
	month := int(t.Month())

	// Months since epoch: (year - 1970) * 12 + (month - 1)
	icebergValue := (year-1970)*12 + (month - 1)

	return TransformResult{
		PathValue:    t.Format("2006-01"),
		IcebergValue: icebergValue,
	}
}

// applyDay extracts date from timestamp.
// Iceberg value: days since 1970-01-01 (e.g., 2024-12-19 → 20077)
// Path value: "2024-12-19"
func applyDay(value any) TransformResult {
	t := toTime(value)
	if t.IsZero() {
		return TransformResult{PathValue: "", IcebergValue: nil}
	}

	// Days since Unix epoch using integer division for precision
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	days := int(t.Sub(epoch) / (24 * time.Hour))

	return TransformResult{
		PathValue:    t.Format("2006-01-02"),
		IcebergValue: days,
	}
}

// applyHour extracts date-hour from timestamp.
// Iceberg value: hours since 1970-01-01 00:00 (e.g., 2024-12-19 10:00 → 481834)
// Path value: "2024-12-19-10"
func applyHour(value any) TransformResult {
	t := toTime(value)
	if t.IsZero() {
		return TransformResult{PathValue: "", IcebergValue: nil}
	}

	// Hours since Unix epoch using integer division for precision
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	hours := int(t.Sub(epoch) / time.Hour)

	return TransformResult{
		PathValue:    t.Format("2006-01-02-15"),
		IcebergValue: hours,
	}
}

// toTime converts various timestamp representations to time.Time.
// For numeric types, uses magnitude-based heuristics to detect unit:
//   - < 1e11: seconds (covers dates up to year ~5138)
//   - 1e11 to 1e14: milliseconds
//   - >= 1e14: microseconds
//
// This matches common conventions where seconds are ~1.7e9 for year 2024,
// milliseconds are ~1.7e12, and microseconds are ~1.7e15.
func toTime(value any) time.Time {
	switch v := value.(type) {
	case time.Time:
		return v.UTC()
	case int64:
		return intToTime(v).UTC()
	case int:
		return intToTime(int64(v)).UTC()
	case float64:
		// we are Assuming seconds since epoch with fractional part
		// Floor to handle negative values correctly
		// while don't if this ever happen just being defensive here.
		// while it's also uncommon to have a float64 but some client like python time.Time() emits.
		sec := int64(math.Floor(v))
		nsec := int64((v - math.Floor(v)) * 1e9)
		return time.Unix(sec, nsec).UTC()
	case string:
		// Try common formats
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t.UTC()
			}
		}
		return time.Time{}
	default:
		return time.Time{}
	}
}

// intToTime converts an integer timestamp to time.Time using magnitude-based unit detection.
// Thresholds are carefully chosen based on realistic date ranges:
//   - Year 2024 in seconds: ~1.7e9
//   - Year 2024 in milliseconds: ~1.7e12
//   - Year 2024 in microseconds: ~1.7e15
//
// We use conservative thresholds:
//   - Seconds: 0 to 1e11 (covers dates up to year ~5138)
//   - Milliseconds: 1e11 to 1e14 (year 1973 to year 5138 in millis)
//   - Microseconds: >= 1e14
func intToTime(v int64) time.Time {
	// dates before 1970
	if v < 0 {
		return time.Unix(v, 0)
	}

	const (
		maxSeconds      = int64(1e11) - 1
		maxMilliseconds = int64(1e14) - 1
	)

	switch {
	case v <= maxSeconds:
		// Likely seconds
		return time.Unix(v, 0)
	case v <= maxMilliseconds:
		// Likely milliseconds
		return time.UnixMilli(v)
	default:
		// Likely microseconds
		return time.UnixMicro(v)
	}
}

// ApplyTransformForPath is a convenience function that returns only the path value.
func ApplyTransformForPath(value any, transform PartitionTransform, param int) string {
	return ApplyTransform(value, transform, param).PathValue
}

// ApplyTransformForIceberg is a convenience function that returns only the Iceberg value.
func ApplyTransformForIceberg(value any, transform PartitionTransform, param int) any {
	return ApplyTransform(value, transform, param).IcebergValue
}

// escapePartitionValue escapes special characters in partition values for Hive-style paths.
// Characters that need escaping: / = % (and control characters)
// This follows Hive's escaping convention for partition values.
//
//	Source: https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/common/FileUtils.java#L254
//
// /**
//   - ASCII 01-1F are HTTP control characters that need to be escaped.
//   - \u000A and \u000D are \n and \r, respectively.
//     */
//     char[] clist = new char[] {'\u0001', '\u0002', '\u0003', '\u0004',
//     '\u0005', '\u0006', '\u0007', '\u0008', '\u0009', '\n', '\u000B',
//     '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012',
//     '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019',
//     '\u001A', '\u001B', '\u001C', '\u001D', '\u001E', '\u001F',
//     '"', '#', '%', '\”, '*', '/', ':', '=', '?', '\\', '\u007F', '{',
//     '[', ']', '^'};
//
// currently we are just supporting subset which should be sufficient for now.
func escapePartitionValue(s string) string {
	var b strings.Builder
	b.Grow(len(s))

	for _, r := range s {
		switch r {
		case '/':
			b.WriteString("%2F")
		case '=':
			b.WriteString("%3D")
		case '%':
			b.WriteString("%25")
		case '\n':
			b.WriteString("%0A")
		case '\r':
			b.WriteString("%0D")
		case '\t':
			b.WriteString("%09")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// BuildPartitionPath builds a Hive-style partition path from partition spec and row values.
// Example: "dt=2024-12-19/region=us-west"
func BuildPartitionPath(partitionSpec []CompiledPartitionField, row []any) string {
	if len(partitionSpec) == 0 || len(row) == 0 {
		return ""
	}

	var parts []string
	for _, pf := range partitionSpec {
		if pf.ColumnIndex >= len(row) {
			continue
		}

		if pf.Transform == TransformVoid {
			continue
		}

		value := row[pf.ColumnIndex]
		result := ApplyTransform(value, pf.Transform, pf.Param)

		if result.PathValue != "" {
			escapedValue := escapePartitionValue(result.PathValue)
			parts = append(parts, fmt.Sprintf("%s=%s", pf.Name, escapedValue))
		}
	}

	if len(parts) == 0 {
		return ""
	}

	return strings.Join(parts, "/")
}

// BuildPartitionTuple builds Iceberg partition values for manifest metadata.
func BuildPartitionTuple(partitionSpec []CompiledPartitionField, row []any) map[string]any {
	if len(partitionSpec) == 0 || len(row) == 0 {
		return nil
	}

	tuple := make(map[string]any, len(partitionSpec))
	for _, pf := range partitionSpec {
		if pf.ColumnIndex >= len(row) {
			continue
		}

		value := row[pf.ColumnIndex]
		result := ApplyTransform(value, pf.Transform, pf.Param)
		tuple[pf.Name] = result.IcebergValue
	}

	return tuple
}
