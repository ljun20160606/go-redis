package pool

import (
	"context"
	"errors"
	"maps"
	"sync"
	"sync/atomic"

	redisvar "github.com/redis/go-redis/v9/var"
)

var _ Pooler = (*ProtoPool)(nil)

type ProtoPool struct {
	sync.Mutex

	opt   *Options
	group map[int]*ConnPool

	_closed uint32 // atomic
}

func NewProtoConnPool(opt *Options) *ProtoPool {
	return &ProtoPool{
		opt:   opt,
		group: map[int]*ConnPool{},
	}
}

func (p *ProtoPool) NewConn(ctx context.Context) (*Conn, error) {
	return p.loadConnPool(ctx).NewConn(ctx)
}

func (p *ProtoPool) CloseConn(conn *Conn) error {
	if pool := p.findConnPool(conn); pool != nil {
		return pool.CloseConn(conn)
	}
	return conn.Close()
}

func (p *ProtoPool) Get(ctx context.Context) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}
	return p.loadConnPool(ctx).Get(ctx)
}

func (p *ProtoPool) Put(ctx context.Context, conn *Conn) {
	if pool := p.findConnPool(conn); pool != nil {
		pool.Put(ctx, conn)
	}
}

func (p *ProtoPool) Remove(ctx context.Context, conn *Conn, err error) {
	if pool := p.findConnPool(conn); pool != nil {
		pool.Remove(ctx, conn, err)
	}
}

func (p *ProtoPool) Len() int {
	var ret int
	for _, g := range p.group {
		ret += g.Len()
	}
	return ret
}

func (p *ProtoPool) IdleLen() int {
	var ret int
	for _, g := range p.group {
		ret += g.IdleLen()
	}
	return ret
}

func (p *ProtoPool) Stats() *Stats {
	stats := &Stats{}
	for _, g := range p.group {
		s := g.Stats()
		stats.Hits += s.Hits
		stats.Misses += s.Misses
		stats.Timeouts += s.Timeouts

		stats.TotalConns += s.TotalConns
		stats.IdleConns += s.IdleConns
		stats.StaleConns += s.StaleConns
	}
	return stats
}

func (p *ProtoPool) Close() error {
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}
	p.Lock()
	defer p.Unlock()
	var es []error
	for _, g := range p.group {
		if err := g.Close(); err != nil {
			es = append(es, err)
		}
	}
	return errors.Join(es...)
}

func (p *ProtoPool) Filter(fn func(*Conn) bool) error {
	var es []error
	for _, g := range p.group {
		es = append(es, g.Filter(fn))
	}
	return errors.Join(es...)
}

func (p *ProtoPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

func (p *ProtoPool) loadConnPool(ctx context.Context) *ConnPool {
	k := loadContext(ctx)
	p.Lock()
	defer p.Unlock()
	if val, has := p.group[k]; has {
		return val
	}
	newG := maps.Clone(p.group)
	pool := NewConnPool(p.opt)
	newG[k] = pool
	p.group = newG
	return pool
}

func (p *ProtoPool) findConnPool(conn *Conn) *ConnPool {
	for _, g := range p.group {
		if g.Id == conn.PoolId {
			return g
		}
	}
	return nil
}

func loadContext(ctx context.Context) int {
	value := ctx.Value(redisvar.ConnVar{})
	if value == nil {
		return 2 // proto 2
	}
	if i, ok := value.(int); ok {
		if i < 2 {
			return 2
		}
		return i
	}
	return 2
}
