package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServerConfig controls HTTP observability endpoint behaviour.
type ServerConfig struct {
	Address        string
	Logger         *slog.Logger
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	ShutdownPeriod time.Duration
	MetricsPath    string
	HealthPath     string
	Metrics        *Metrics
}

// Server hosts metrics and health endpoints.
type Server struct {
	cfg ServerConfig
	srv *http.Server
}

// NewServer prepares an observability HTTP server.
func NewServer(cfg ServerConfig) *Server {
	if cfg.Address == "" {
		cfg.Address = ":2112"
	}
	if cfg.Logger == nil {
		cfg.Logger = NoOpLogger()
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	if cfg.ShutdownPeriod == 0 {
		cfg.ShutdownPeriod = 5 * time.Second
	}
	if cfg.MetricsPath == "" {
		cfg.MetricsPath = "/metrics"
	}
	if cfg.HealthPath == "" {
		cfg.HealthPath = "/healthz"
	}

	mux := http.NewServeMux()
	mux.Handle(cfg.MetricsPath, promhttp.Handler())
	mux.HandleFunc(cfg.HealthPath, func(w http.ResponseWriter, r *http.Request) {
		if cfg.Metrics != nil && !cfg.Metrics.Healthy() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("unhealthy\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	srv := &http.Server{
		Addr:         cfg.Address,
		Handler:      mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return &Server{cfg: cfg, srv: srv}
}

// Run starts serving HTTP requests until the context is cancelled.
func (s *Server) Run(ctx context.Context) {
	if s == nil {
		return
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownPeriod)
		defer cancel()
		if err := s.srv.Shutdown(shutdownCtx); err != nil {
			s.cfg.Logger.Error("observability server shutdown error", slog.Any("error", err))
		}
	}()

	s.cfg.Logger.Info("observability server listening", slog.String("address", s.cfg.Address))
	if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.cfg.Logger.Error("observability server stopped unexpectedly", slog.Any("error", err))
	}
}
