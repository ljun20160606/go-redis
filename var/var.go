package redisvar

import (
	"context"

	"github.com/redis/go-redis/v9/internal/apis"
)

func WithConnVar(ctx context.Context, v int) context.Context {
	return context.WithValue(ctx, apis.ConnVar{}, v)
}
