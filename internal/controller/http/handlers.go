package http

import (
	"myapp/internal/usecase"
)

// Обработчики HTTP запросов
type Handlers struct {
	profileService *usecase.Profile
}

func NewHandlers(profileService *usecase.Profile) *Handlers {
	return &Handlers{
		profileService: profileService,
	}
}
