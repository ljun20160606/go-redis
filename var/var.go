package redisvar

import "context"

type ConnVar struct {
}

func WithConnVar(ctx context.Context, v int) context.Context {
	return context.WithValue(ctx, ConnVar{}, v)
}
