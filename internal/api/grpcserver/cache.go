package grpcserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type cacheLayer interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Close() error
}

type noopCache struct{}

func (noopCache) Get(_ context.Context, _ string) ([]byte, bool, error) {
	return nil, false, nil
}

func (noopCache) Set(_ context.Context, _ string, _ []byte, _ time.Duration) error {
	return nil
}

func (noopCache) Close() error {
	return nil
}

type redisCache struct {
	client     *redis.Client
	defaultTTL time.Duration
}

func newRedisCache(cfg CacheConfig) (cacheLayer, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.RedisAddress == "" {
		return nil, fmt.Errorf("grpc cache: redis address must be provided when cache is enabled")
	}

	opts := &redis.Options{
		Addr:     cfg.RedisAddress,
		Username: cfg.RedisUsername,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	}

	if cfg.RedisTLSEnabled {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: cfg.RedisInsecureSkipVerify, // #nosec G402 — controlled via config for trusted environments.
		}
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("grpc cache: ping redis: %w", err)
	}

	ttl := time.Duration(cfg.DefaultTTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 30 * time.Second
	}

	return &redisCache{
		client:     client,
		defaultTTL: ttl,
	}, nil
}

func (c *redisCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	cmd := c.client.Get(ctx, key)
	if err := cmd.Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		return nil, false, err
	}
	data, err := cmd.Bytes()
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

func (c *redisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	return c.client.Set(ctx, key, value, ttl).Err()
}

func (c *redisCache) Close() error {
	return c.client.Close()
}
