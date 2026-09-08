package http

import (
	"encoding/json"
	"log/slog"
	"myapp/pkg/render"
	"net/http"
)

func (h *Handlers) CreateProfile(w http.ResponseWriter, r *http.Request) {
	log := slog.Default()
	type Input struct {
		Name  string `json:"name"`
		Age   int    `json:"age"`
		Email string `json:"email"`
	}

	input := Input{}

	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Error("error while decoding input")
		return
	}

	output, err := h.profileService.CreateProfile(r.Context(), input.Name, input.Age, input.Email)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Error("error while creating profile http.StatusBadRequest")
		return
	}

	render.JSON(w, output, http.StatusOK)
}
