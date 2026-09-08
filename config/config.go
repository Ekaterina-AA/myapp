package config

import (
	"myapp/internal/adapter/kafka_produce"
	"myapp/internal/adapter/postgres"
	"myapp/internal/adapter/redis"
	"myapp/internal/controller/kafka_consume"
	"myapp/pkg/httpserver"
	"myapp/pkg/logger"
	"myapp/pkg/otel"

	"github.com/kelseyhightower/envconfig"
)

type App struct {
	Name    string `envconfig:"APP_NAME"    required:"true"`
	Version string `envconfig:"APP_VERSION" required:"true"`
}

type Config struct {
	App           App
	HTTP          httpserver.Config
	Logger        logger.Config
	OTEL          otel.Config
	Postgres      postgres.Config
	Redis         redis.Config
	KafkaProducer kafka_produce.Config
	KafkaConsumer kafka_consume.Config
}

func InitConfig() (Config, error) {

	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil

}
