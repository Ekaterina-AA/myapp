package kafka_consume

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"myapp/internal/usecase"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	Addr  []string `envconfig:"KAFKA_CONSUMER_ADDR"     required:"true"`
	Topic string   `envconfig:"KAFKA_CONSUMER_TOPIC"`
	Group string   `envconfig:"KAFKA_CONSUMER_GROUP"`
}

// Кафка консьюмер
type Consumer struct {
	config  Config
	reader  *kafka.Reader
	profile *usecase.Profile
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func New(cfg Config, profile *usecase.Profile) *Consumer {
	// Настройки
	ctx, cancel := context.WithCancel(context.Background())

	// Настройка reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         cfg.Addr,
		GroupID:         cfg.Group,
		Topic:           cfg.Topic,
		MinBytes:        10e3, // 10KB
		MaxBytes:        10e6, // 10MB
		MaxWait:         1 * time.Second,
		ReadLagInterval: 3 * time.Second,
		CommitInterval:  0, // synchronous commits
		StartOffset:     kafka.LastOffset,
		RetentionTime:   24 * time.Hour,
	})

	consumer := &Consumer{
		config:  cfg,
		reader:  reader,
		profile: profile,
		ctx:     ctx,
		cancel:  cancel,
	}

	// Запуск горутины для приёма сообщений и передачи их в сервис
	consumer.wg.Add(1)
	go consumer.consumeLoop()

	log.Printf("Kafka consumer started for topic: %s, group: %s", cfg.Topic, cfg.Group)
	return consumer
}

func (c *Consumer) consumeLoop() {
	log := slog.Default()
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			log.Info("Stopping Kafka consumer...")
			return
		default:
			// Читаем сообщение с таймаутом
			msg, err := c.reader.ReadMessage(c.ctx)
			if err != nil {
				if c.ctx.Err() != nil {
					return // Context cancelled
				}
				log.Error("Error reading message", slog.String("key", string(msg.Key)))
				time.Sleep(1 * time.Second)
				continue
			}

			// Обрабатываем сообщение
			if err := c.handleMessage(c.ctx, msg); err != nil {
				log.Error("Error handling message", slog.String("key", string(msg.Key)))
				continue
			}

			// Коммитим смещение только после успешной обработки
			if err := c.reader.CommitMessages(c.ctx, msg); err != nil {
				log.Error("Error committing message", slog.String("key", string(msg.Key)))
			}
		}
	}
}

// handleMessage обрабатывает отдельное сообщение
func (c *Consumer) handleMessage(ctx context.Context, msg kafka.Message) error {
	log := slog.Default()

	// Определяем тип события по ключу
	switch string(msg.Key) {
	case "profile.created":
		return c.handleProfileCreated(ctx, msg.Value)
	default:
		log.Warn("Unknown event type",
			slog.String("key", string(msg.Key)),
		)
		return nil
	}
}

// handleProfileCreated обрабатывает событие создания профиля
func (c *Consumer) handleProfileCreated(ctx context.Context, value []byte) error {
	var event ProfileCreatedEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal profile created event: %w", err)
	}

	log.Printf("Processing profile created event: %+v", event)

	// Вызываем метод usecase для обработки события
	fmt.Println("kafka consumer action")

	return nil
}

// Close gracefully останавливает consumer
func (c *Consumer) Close() {
	log := slog.Default()
	log.Info("Closing Kafka consumer...")

	// Отменяем контекст для остановки consumeLoop
	c.cancel()

	// Ждем завершения consumeLoop
	c.wg.Wait()

	// Закрываем reader
	if err := c.reader.Close(); err != nil {
		log.Error("Error closing Kafka reader")
	}

	log.Info("Kafka consumer closed")
}

type ProfileCreatedEvent struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Age       int       `json:"age"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}
