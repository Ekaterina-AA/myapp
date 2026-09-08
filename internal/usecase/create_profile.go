package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"myapp/internal/adapter/kafka_produce"
	"myapp/internal/domain"

	"github.com/google/uuid"
)

func (p *Profile) CreateProfile(ctx context.Context, name string, age int, email string) (uuid.UUID, error) {
	log := slog.Default()

	// Проверяем в Redis ключ идемпотентности
	if p.redis.IsExists(ctx, name+email) {
		return uuid.Nil, domain.ErrAlreadyExists
	}

	// Создаём профиль
	profile := domain.CustomerProfile{
		ID:        uuid.New(),
		CreatedAt: time.Now(),
		Name:      name,
		Age:       age,
		Email:     email,
	}

	// Валидируем
	err := profile.Validate()
	if err != nil {
		log.Error("invalid profile")
		return uuid.Nil, fmt.Errorf("validate profile: %w", err)
	}

	// Сохраняем в БД
	err = p.postgres.CreateProfile(ctx, profile)
	if err != nil {
		log.Error("profile doesnt exist in postgres")
		return uuid.Nil, fmt.Errorf("failed to create profile in postgres: %w", err)
	}

	idempotencyKey := fmt.Sprintf("idempotent:%s:%s", name, email)

	// Асинхронно отправляем в Redis событие создания профиля
	go func() {
		redisCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		// Сохраняем ключ идемпотентности с TTL 5 минут
		if err := p.redis.Set(redisCtx, idempotencyKey, profile.ID.String(), 5*time.Minute); err != nil {
			log.Warn("failed to save idempotency key in Redis",
				slog.String("key", idempotencyKey),
				slog.String("error", err.Error()),
			)
		}

		// Сохраняем профиль в кэш
		cacheKey := fmt.Sprintf("profile:%s", profile.ID)
		if err := p.redis.Set(redisCtx, cacheKey, profile, 10*time.Minute); err != nil {
			log.Warn("failed to cache profile in Redis",
				slog.String("id", profile.ID.String()),
				slog.String("error", err.Error()),
			)
		} else {
			log.Debug("profile cached in Redis",
				slog.String("id", profile.ID.String()),
				slog.String("key", cacheKey),
			)
		}
	}()

	// Асинхронно отправляем событие в Kafka
	go func() {
		// Создаем отдельный контекст с таймаутом
		kafkaCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Формируем событие
		event := map[string]interface{}{
			"event_type": "profile.created",
			"profile": map[string]interface{}{
				"id":         profile.ID.String(),
				"name":       profile.Name,
				"age":        profile.Age,
				"email":      profile.Email,
				"created_at": profile.CreatedAt,
			},
			"timestamp": time.Now(),
			"service":   "profile-service",
		}
		// Отправляем в Kafka
		if err := p.kafka.Produce(kafkaCtx, kafka_produce.Message{
			Key:       "profile.created",
			Value:     event,
			Timestamp: time.Now(),
		}); err != nil {
			log.Error("failed to send event to Kafka",
				slog.String("id", profile.ID.String()),
				slog.String("error", err.Error()),
			)
		} else {
			log.Info("event published to Kafka",
				slog.String("id", profile.ID.String()),
				slog.String("event_type", "profile.created"),
			)
		}
	}()

	return profile.ID, nil
}
