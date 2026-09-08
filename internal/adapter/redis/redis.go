package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"myapp/internal/domain"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Addr         string        `envconfig:"REDIS_ADDR"  required:"true"`
	Password     string        `envconfig:"REDIS_PASSWORD"`
	DB           int           `default:"0"  envconfig:"REDIS_DB"`
	PoolSize     int           `default:"10" envconfig:"REDIS_POOL_SIZE"`
	MinIdleConns int           `default:"5" envconfig:"REDIS_MIN_IDLE_CONNS"`
	MaxRetries   int           `default:"3" envconfig:"REDIS_MAX_RETRIES"`
	DialTimeout  time.Duration `default:"5s" envconfig:"REDIS_DIAL_TIMEOUT"`
	ReadTimeout  time.Duration `default:"3s" envconfig:"REDIS_READ_TIMEOUT"`
	WriteTimeout time.Duration `default:"3s" envconfig:"REDIS_WRITE_TIMEOUT"`
	PoolTimeout  time.Duration `default:"4s" envconfig:"REDIS_POOL_TIMEOUT"`
}

type Client struct {
	client *redis.Client
	config Config
}

func New(c Config) (*Client, error) {
	log := slog.Default()

	// Делаем подключение к Redis
	log.Info("Initializing Redis client",
		slog.String("addr", c.Addr),
		slog.Int("db", c.DB),
	)

	client := redis.NewClient(&redis.Options{
		Addr:            c.Addr,
		Password:        c.Password,
		DB:              c.DB,
		PoolSize:        c.PoolSize,
		MinIdleConns:    c.MinIdleConns,
		MaxRetries:      c.MaxRetries,
		DialTimeout:     c.DialTimeout,
		ReadTimeout:     c.ReadTimeout,
		WriteTimeout:    c.WriteTimeout,
		PoolTimeout:     c.PoolTimeout,
		ConnMaxLifetime: time.Hour,
	})

	// Проверяем подключение
	ctx, cancel := context.WithTimeout(context.Background(), c.DialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		log.Error("Failed to connect to Redis",
			slog.String("error", err.Error()),
			slog.String("addr", c.Addr),
		)
		return nil, fmt.Errorf("%w: %v", domain.ErrRedisConnection, err)
	}

	log.Info("Redis client initialized successfully",
		slog.String("addr", c.Addr),
		slog.Int("db", c.DB),
	)

	return &Client{
		client: client,
		config: c,
	}, nil
}

func (c *Client) IsExists(ctx context.Context, idempotencyKey string) bool {
	// Проверяем, существует ли ключ в Redis
	if idempotencyKey == "" {
		return false
	}

	key := c.idempotencyKey(idempotencyKey)
	exists, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		slog.Warn("Failed to check idempotency key",
			slog.String("key", idempotencyKey),
			slog.String("error", err.Error()),
		)
		return false
	}

	return exists > 0
}

func (c *Client) idempotencyKey(key string) string {
	return fmt.Sprintf("idempotency:%s", key)
}

func (c *Client) Ping(ctx context.Context) error {
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}
	return nil
}

func (c *Client) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	log := slog.Default()
	if key == "" {
		return errors.New("key cannot be empty")
	}

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	if err := c.client.Set(ctx, key, data, ttl).Err(); err != nil {
		log.Error("Failed to set cache",
			slog.String("key", key),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("set cache: %w", err)
	}

	log.Debug("Cache set",
		slog.String("key", key),
		slog.Duration("ttl", ttl),
	)

	return nil
}

func (c *Client) Get(ctx context.Context, key string, dest interface{}) error {
	log := slog.Default()

	if key == "" {
		return errors.New("key cannot be empty")
	}

	data, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return domain.ErrNilKeyRedis // ключ не найден
		}
		log.Error("Failed to get cache",
			slog.String("key", key),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("get cache: %w", err)
	}

	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("unmarshal value: %w", err)
	}

	log.Debug("Cache hit",
		slog.String("key", key),
	)

	return nil
}
func (c *Client) Delete(ctx context.Context, keys ...string) error {
	return nil
}

func (c *Client) SetIdempotencyKey(ctx context.Context, idempotencyKey string, ttl time.Duration) error {
	if idempotencyKey == "" {
		return errors.New("idempotency key cannot be empty")
	}

	key := c.idempotencyKey(idempotencyKey)

	if err := c.client.Set(ctx, key, "processed", ttl).Err(); err != nil {
		slog.Error("Failed to set idempotency key",
			slog.String("key", idempotencyKey),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("set idempotency key: %w", err)
	}

	slog.Debug("Idempotency key set",
		slog.String("key", idempotencyKey),
		slog.Duration("ttl", ttl),
	)

	return nil
}

func (c *Client) Close() {
	log := slog.Default()
	// Shutdown
	if c.client == nil {
		return
	}

	log.Info("Closing Redis connection",
		slog.String("addr", c.config.Addr),
	)

	if err := c.client.Close(); err != nil {
		log.Error("Failed to close Redis connection",
			slog.String("error", err.Error()),
		)
	} else {
		log.Info("Redis connection closed successfully")
	}
}
