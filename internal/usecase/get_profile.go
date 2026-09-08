package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"myapp/internal/domain"

	"github.com/google/uuid"
)

func (p *Profile) GetProfile(ctx context.Context, id string) (domain.CustomerProfile, error) {
	log := slog.Default()
	// Валидируем ID
	profileID, err := uuid.Parse(id)
	if err != nil {
		log.Error("invalid id")
		return domain.CustomerProfile{}, domain.ErrUUIDInvalid
	}

	cacheKey := fmt.Sprintf("profile:%s", profileID)

	var cachedProfile domain.CustomerProfile
	err = p.redis.Get(ctx, cacheKey, &cachedProfile)
	if err == nil {
		log.Info("profile found in Redis cache", slog.String("id", profileID.String()))
		return cachedProfile, nil
	}

	// Если в Redis ошибка (не nil), логируем, но продолжаем
	if err != nil && err != domain.ErrNilKeyRedis {
		log.Warn("redis get error", slog.String("key", cacheKey), slog.String("error", err.Error()))
	}

	// Достаем профиль из postgres
	profile, err := p.postgres.GetProfile(ctx, profileID)
	if err != nil {
		log.Error("get profile from postgres error")
		return domain.CustomerProfile{}, fmt.Errorf("get profile from postgres: %w", err)
	}

	// Сохраняем в Redis (асинхронно, не блокируем ответ)
	go func() {
		// Создаем контекст с таймаутом для Redis операции
		redisCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := p.redis.Set(redisCtx, cacheKey, profile, 10*time.Minute); err != nil {
			log.Warn("failed to cache profile in Redis",
				slog.String("id", profileID.String()),
				slog.String("error", err.Error()),
			)
		} else {
			log.Debug("profile cached in Redis",
				slog.String("id", profileID.String()),
				slog.String("key", cacheKey),
			)
		}
	}()

	return profile, nil
}
