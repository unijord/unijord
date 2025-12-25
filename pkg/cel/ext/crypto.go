package ext

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/cespare/xxhash/v2"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// CryptoFuncs returns CEL environment options for hashing functions.
//
// Functions:
//   - xxhash(string) -> string: xxHash64 as hex (fast, non-cryptographic, 16 chars)
//   - sha256(string) -> string: SHA256 as hex (cryptographic, 64 chars)
func CryptoFuncs() cel.EnvOption {
	return cel.Lib(&cryptoLib{})
}

type cryptoLib struct{}

func (l *cryptoLib) LibraryName() string {
	return "unijord.crypto"
}

func (l *cryptoLib) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function("xxhash",
			cel.Overload("xxhash_string",
				[]*cel.Type{cel.StringType},
				cel.StringType,
				cel.UnaryBinding(func(s ref.Val) ref.Val {
					sum := xxhash.Sum64String(string(s.(types.String)))
					return types.String(strconv.FormatUint(sum, 16))
				}),
			),
		),
		cel.Function("sha256",
			cel.Overload("sha256_string",
				[]*cel.Type{cel.StringType},
				cel.StringType,
				cel.UnaryBinding(func(s ref.Val) ref.Val {
					h := sha256.Sum256([]byte(string(s.(types.String))))
					return types.String(hex.EncodeToString(h[:]))
				}),
			),
		),
	}
}

func (l *cryptoLib) ProgramOptions() []cel.ProgramOption {
	return nil
}
