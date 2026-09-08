package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"myapp/internal/domain"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type Config struct {
	User     string `envconfig:"POSTGRES_USER"     required:"true"`
	Password string `envconfig:"POSTGRES_PASSWORD" required:"true"`
	Port     string `envconfig:"POSTGRES_PORT"     required:"true"`
	Host     string `envconfig:"POSTGRES_HOST"     required:"true"`
	DBName   string `envconfig:"POSTGRES_DB_NAME"  required:"true"`
}

type Pool struct {
	conn *sql.DB
}

func New(ctx context.Context, c Config) (*Pool, error) {
	log := slog.Default()
	// Делаем настройки подключения и пингуем БД на доступность
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.User,
		c.Password,
		c.Host,
		c.Port,
		c.DBName,
	)

	db, err := sql.Open("postgres", connString)
	if err != nil {
		log.Error("failed to open database")
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)
	db.SetConnMaxIdleTime(30 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		log.Error("failed to ping database")
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Pool{conn: db}, nil
}

func (p *Pool) CreateProfile(ctx context.Context, profile domain.CustomerProfile) error {
	query := `
		INSERT INTO profiles (id, name, age, email, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err := p.conn.ExecContext(ctx, query,
		profile.ID,
		profile.Name,
		profile.Age,
		profile.Email,
		profile.CreatedAt,
	)

	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return domain.ErrAlreadyExists
		}
		return fmt.Errorf("failed to create profile: %w", err)
	}

	return nil
}

func (p *Pool) GetProfile(ctx context.Context, id uuid.UUID) (domain.CustomerProfile, error) {
	query := `
		SELECT id, name, age, email, created_at
		FROM profiles
		WHERE id = $1
	`

	var profile domain.CustomerProfile
	err := p.conn.QueryRowContext(ctx, query, id).Scan(
		&profile.ID,
		&profile.Name,
		&profile.Age,
		&profile.Email,
		&profile.CreatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return domain.CustomerProfile{}, fmt.Errorf("profile not found: %w", err)
		}
		return domain.CustomerProfile{}, fmt.Errorf("failed to get profile: %w", err)
	}

	return profile, nil
}

func (p *Pool) Close() {
	// Shutdown
	if p.conn != nil {
		p.conn.Close()
	}
}
