package domain

import (
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

type CustomerProfile struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	Name      string    `json:"name"  validate:"required,min=3,max=64"`
	Age       int       `json:"age"   validate:"required,min=18,max=120"`
	Email     string    `json:"email" validate:"email"`
}

func (p CustomerProfile) Validate() error {
	log := slog.Default()

	// 1. Проверка ID (должен быть валидным UUID)
	if p.ID == uuid.Nil {
		log.Error("invalid id")
		return ErrUUIDInvalid
	}

	// 2. Проверка CreatedAt (не должно быть нулевым и не в будущем)
	if p.CreatedAt.IsZero() {
		log.Error("invalid CreatedAt, нулевое")
		return ErrTimeInvalid
	}
	if p.CreatedAt.After(time.Now()) {
		log.Error("invalid CreatedAt, дата в будущем")
		return ErrTimeInvalid
	}

	// 3. Проверка Name (required, min=3, max=64)
	name := strings.TrimSpace(p.Name)
	if name == "" {
		log.Error("invalid Name, отсутствие Name")
		return ErrNameInvalid
	}
	if len(name) < 3 {
		log.Error("invalid Name, длина меньше 3")
		return ErrNameInvalid
	}
	if len(name) > 64 {
		log.Error("invalid Name, длина больше 64")
		return ErrNameInvalid
	}

	// 4. Проверка Age (required, min=18, max=120)
	if p.Age < 18 {
		log.Error("invalid Age, меньше 18")
		return ErrAgeInvalid
	}
	if p.Age > 120 {
		log.Error("invalid Age, больше 120")
		return ErrAgeInvalid
	}

	// 5. Проверка Email (обязательный и валидный формат)
	email := strings.TrimSpace(p.Email)
	if email == "" {
		log.Error("invalid Email, отсутствие email")
		return ErrEmailInvalid
	}
	if !isValidEmail(email) {
		log.Error("invalid Email, невалидный формат")
		return ErrEmailInvalid
	}

	return nil
}

// isValidEmail проверяет формат email
func isValidEmail(email string) bool {
	// Простая проверка email
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)
	return emailRegex.MatchString(email)
}
