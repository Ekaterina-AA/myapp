package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"myapp/config"
	"myapp/internal/adapter/kafka_produce"
	"myapp/internal/adapter/postgres"
	"myapp/internal/adapter/redis"
	"myapp/internal/controller/http"
	"myapp/internal/controller/kafka_consume"
	"myapp/internal/usecase"
	"myapp/pkg/httpserver"
	"myapp/pkg/logger"
)

func main() {
	os.Setenv("APP_NAME", "MYAPP")
	os.Setenv("APP_VERSION", "1.0")

	os.Setenv("KAFKA_WRITER_ADDR", "localhost:9092")
	os.Setenv("KAFKA_WRITER_TOPIC", "myapp-profiles")

	os.Setenv("KAFKA_CONSUMER_ADDR", "localhost:9092")
	os.Setenv("KAFKA_CONSUMER_TOPIC", "myapp-profiles")
	os.Setenv("KAFKA_CONSUMER_GROUP", "myapp-group")

	os.Setenv("POSTGRES_USER", "postgres")
	os.Setenv("POSTGRES_PASSWORD", "admin")
	os.Setenv("POSTGRES_PORT", "5432")
	os.Setenv("POSTGRES_HOST", "localhost")
	os.Setenv("POSTGRES_DB_NAME", "DBNAME")

	os.Setenv("HTTP_PORT", "8080")

	os.Setenv("LOGGER_LEVEL", "error")
	os.Setenv("LOGGER_PRETTY_CONSOLE", "false")

	c, err := config.InitConfig()
	if err != nil {
		panic(err)
	}

	logger.Init(c.Logger)

	err = AppRun(context.Background(), c)
	if err != nil {
		panic(err)
	}

}

func AppRun(ctx context.Context, cfg config.Config) error {

	// Postgres
	pgPool, err := postgres.New(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("postgres.New: %w", err)
	}

	// Redis
	redisClient, err := redis.New(cfg.Redis)
	if err != nil {
		return fmt.Errorf("redis.New: %w", err)
	}

	// Kafka producer
	kafkaProducer := kafka_produce.NewProducer(cfg.KafkaProducer)

	// Usecase (Service)
	profileUsecase := usecase.NewProfile(pgPool, kafkaProducer, redisClient)

	// Kafka consumer
	kafkaConsumer := kafka_consume.New(cfg.KafkaConsumer, profileUsecase)

	// HTTP сервер
	router := http.Router(profileUsecase)
	httpServer := httpserver.New(router, cfg.HTTP)

	// Приложение запущено и готово к работе

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	<-sig // ждём здесь сигнала (Ctrl+C или SIGTERM)

	// Закрываем ресурсы
	kafkaConsumer.Close()
	httpServer.Close()
	redisClient.Close()
	kafkaProducer.Close()
	pgPool.Close()

	return nil
}
