package cel

import (
	"github.com/google/cel-go/cel"
)

type EnvBuilder struct {
	opts []cel.EnvOption
	err  error
}

func NewEnvBuilder() *EnvBuilder {
	return &EnvBuilder{}
}

func (b *EnvBuilder) WithVariable(name string, t *cel.Type) *EnvBuilder {
	if b.err != nil {
		return b
	}
	b.opts = append(b.opts, cel.Variable(name, t))
	return b
}

func (b *EnvBuilder) WithEnvelope() *EnvBuilder {
	if b.err != nil {
		return b
	}
	b.opts = append(b.opts,
		cel.Variable("_event_id", cel.BytesType),
		cel.Variable("_event_type", cel.StringType),
		cel.Variable("_occurred_at", cel.IntType),
		cel.Variable("_ingested_at", cel.IntType),
		cel.Variable("_schema_id", cel.IntType),
		cel.Variable("_lsn", cel.IntType),
	)
	return b
}

func (b *EnvBuilder) WithLibrary(lib cel.Library) *EnvBuilder {
	if b.err != nil {
		return b
	}
	b.opts = append(b.opts, cel.Lib(lib))
	return b
}

func (b *EnvBuilder) WithOption(opt cel.EnvOption) *EnvBuilder {
	if b.err != nil {
		return b
	}
	b.opts = append(b.opts, opt)
	return b
}

func (b *EnvBuilder) Build() (*cel.Env, error) {
	if b.err != nil {
		return nil, b.err
	}
	return cel.NewEnv(b.opts...)
}
