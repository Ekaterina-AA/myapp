package usecase

import (
	"context"
	"myapp/internal/adapter/kafka_produce"
	"myapp/internal/adapter/postgres"
	"myapp/internal/adapter/redis"
	"myapp/internal/domain"
	"time"

	"github.com/google/uuid"
)

type Redis interface {
	IsExists(ctx context.Context, idempotencyKey string) bool
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	Get(ctx context.Context, key string, dest interface{}) error
}

type Kafka interface {
	Produce(ctx context.Context, msgs ...kafka_produce.Message) error
}

type Postgres interface {
	CreateProfile(ctx context.Context, profile domain.CustomerProfile) error
	GetProfile(ctx context.Context, id uuid.UUID) (domain.CustomerProfile, error)
}

type Profile struct {
	postgres Postgres
	kafka    Kafka
	redis    Redis
}

func NewProfile(postgres *postgres.Pool, kafka *kafka_produce.Producer, redis *redis.Client) *Profile {
	return &Profile{
		postgres: postgres,
		kafka:    kafka,
		redis:    redis,
	}
}
