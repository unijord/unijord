package cel

import (
	"sync"

	"github.com/google/cel-go/interpreter"
)

type activationPool struct {
	pool sync.Pool
}

func newActivationPool() *activationPool {
	return &activationPool{
		pool: sync.Pool{
			New: func() any {
				return &pooledActivation{
					vars: make(map[string]any, 16),
				}
			},
		},
	}
}

func (p *activationPool) get(vars map[string]any) *pooledActivation {
	a := p.pool.Get().(*pooledActivation)
	a.reset(vars)
	return a
}

func (p *activationPool) put(a *pooledActivation) {
	clear(a.vars)
	p.pool.Put(a)
}

type pooledActivation struct {
	vars map[string]any
}

func (a *pooledActivation) ResolveName(name string) (any, bool) {
	v, ok := a.vars[name]
	return v, ok
}

func (a *pooledActivation) Parent() interpreter.Activation {
	return nil
}

func (a *pooledActivation) reset(vars map[string]any) {
	clear(a.vars)
	for k, v := range vars {
		a.vars[k] = v
	}
}

var _ interpreter.Activation = (*pooledActivation)(nil)
