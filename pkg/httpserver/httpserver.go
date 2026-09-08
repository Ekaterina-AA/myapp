package httpserver

import (
	"context"
	"log"
	"net/http"
	"time"
)

type Config struct {
	Port         string        `default:"8080" envconfig:"HTTP_PORT"`
	ReadTimeout  time.Duration `default:"5s"`
	WriteTimeout time.Duration `default:"10s"`
	IdleTimeout  time.Duration `default:"120s"`
}

type Server struct {
	server *http.Server
}

func New(handler http.Handler, c Config) *Server {
	// Настраиваем порт и таймауты
	server := &http.Server{
		Addr:         ":" + c.Port,
		Handler:      handler,
		ReadTimeout:  c.ReadTimeout,
		WriteTimeout: c.WriteTimeout,
		IdleTimeout:  c.IdleTimeout,
	}

	return &Server{
		server: server,
	}
}

func (s *Server) Close() {
	// Shutdown
	if s.server == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}
}
