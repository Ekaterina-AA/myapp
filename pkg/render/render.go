package render

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

func JSON(w http.ResponseWriter, body any, statusCode int) {
	log := slog.Default()
	w.Header().Set("Content-Type", "application/json")

	w.WriteHeader(statusCode)

	err := json.NewEncoder(w).Encode(body)
	if err != nil {
		log.Error("json encode error")
		http.Error(w, "json encode error", http.StatusBadRequest)
		panic(err)
	}
}
