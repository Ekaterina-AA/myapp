package kafka_produce

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Addr  []string `envconfig:"KAFKA_WRITER_ADDR"  required:"true"`
	Topic string   `envconfig:"KAFKA_WRITER_TOPIC"`
}

type Producer struct {
	config Config
	writer *kafka.Writer
}

func NewProducer(c Config) *Producer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(c.Addr...),
		Topic:                  c.Topic,
		Balancer:               &kafka.LeastBytes{},
		WriteTimeout:           10 * time.Second,
		ReadTimeout:            10 * time.Second,
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
		Async:                  false,
		Compression:            kafka.Snappy,
		Logger:                 nil,
		ErrorLogger:            nil,
	}

	return &Producer{
		config: c,
		writer: writer,
	}
}

type Message struct {
	Key       string      `json:"-"`
	Value     interface{} `json:"value"`
	Timestamp time.Time   `json:"timestamp"`
}

func (p *Producer) Produce(ctx context.Context, msgs ...Message) error {
	if len(msgs) == 0 {
		return nil
	}

	kafkaMessages := make([]kafka.Message, len(msgs))
	for i, msg := range msgs {
		valueBytes, err := json.Marshal(msg.Value)
		if err != nil {
			return fmt.Errorf("failed to marshal message value: %w", err)
		}

		kafkaMessage := kafka.Message{
			Value: valueBytes,
			Time:  msg.Timestamp,
		}

		if msg.Key != "" {
			kafkaMessage.Key = []byte(msg.Key)
		}

		kafkaMessages[i] = kafkaMessage
	}

	if len(kafkaMessages) == 1 {
		err := p.writer.WriteMessages(ctx, kafkaMessages[0])
		if err != nil {
			return fmt.Errorf("failed to write message to Kafka: %w", err)
		}
	} else {
		err := p.writer.WriteMessages(ctx, kafkaMessages...)
		if err != nil {
			return fmt.Errorf("failed to write messages to Kafka: %w", err)
		}
	}

	return nil
}

func (p *Producer) Close() {
	// Shutdown
	log := slog.Default()
	log.Info("Closing Kafka producer...")

	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			log.Error("Error closing Kafka writer",
				slog.String("error", err.Error()),
			)
		} else {
			log.Info("Kafka producer closed successfully")
		}
	}
}
