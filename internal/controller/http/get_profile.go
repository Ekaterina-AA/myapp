package http

import (
	"log/slog"
	"myapp/pkg/render"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (h *Handlers) GetProfile(w http.ResponseWriter, r *http.Request) {
	log := slog.Default()
	id := chi.URLParam(r, "id")

	output, err := h.profileService.GetProfile(r.Context(), id)
	if err != nil {
		log.Error("error while getting profile http.StatusBadRequest")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	render.JSON(w, output, http.StatusOK)
}
