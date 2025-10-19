package grpcserver

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net"
	"sync"

	meshpipev1 "github.com/aminovpavel/meshpipe-go/internal/api/grpc/gen/meshpipe/v1"
	"github.com/aminovpavel/meshpipe-go/internal/observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Server wraps a gRPC server instance and SQLite reader.
type Server struct {
	cfg     Config
	logger  *slog.Logger
	metrics *observability.Metrics

	db        *sql.DB
	grpc      *grpc.Server
	startOnce sync.Once
	closeOnce sync.Once
}

// New constructs a Server. If cfg.Enabled is false, the returned server is a no-op.
func New(cfg Config, logger *slog.Logger, metrics *observability.Metrics) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if !cfg.Enabled {
		return &Server{
			cfg:     cfg,
			logger:  logger,
			metrics: metrics,
		}, nil
	}
	if cfg.Address == "" {
		cfg.Address = ":7443"
	}
	if cfg.MaxPageSize <= 0 {
		cfg.MaxPageSize = 500
	}

	db, err := openReadOnlyDB(cfg.DatabasePath)
	if err != nil {
		return nil, err
	}

	service := newService(db, logger, cfg.MaxPageSize)

	opts := []grpc.ServerOption{}
	if cfg.AuthToken != "" {
		opts = append(opts,
			grpc.UnaryInterceptor(tokenUnaryInterceptor(cfg.AuthToken)),
			grpc.StreamInterceptor(tokenStreamInterceptor(cfg.AuthToken)),
		)
	}

	grpcSrv := grpc.NewServer(opts...)
	meshpipev1.RegisterMeshpipeDataServer(grpcSrv, service)

	return &Server{
		cfg:     cfg,
		logger:  logger.With(slog.String("component", "grpc")),
		metrics: metrics,
		db:      db,
		grpc:    grpcSrv,
	}, nil
}

// Run starts serving requests until the context is cancelled. If the server is disabled, Run is a no-op.
func (s *Server) Run(ctx context.Context) {
	if !s.cfg.Enabled {
		return
	}

	var (
		listener net.Listener
		err      error
	)

	s.startOnce.Do(func() {
		listener, err = net.Listen("tcp", s.cfg.Address)
		if err != nil {
			s.logger.Error("failed to listen", slog.String("address", s.cfg.Address), slog.Any("error", err))
			return
		}

		go func() {
			<-ctx.Done()
			s.grpc.GracefulStop()
			if listener != nil {
				_ = listener.Close()
			}
		}()

		s.logger.Info("grpc server listening", slog.String("address", s.cfg.Address))

		if serveErr := s.grpc.Serve(listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			s.logger.Error("grpc server stopped with error", slog.Any("error", serveErr))
		} else {
			s.logger.Info("grpc server stopped")
		}
	})
}

// Close releases resources associated with the server.
func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		if s.db != nil {
			err = s.db.Close()
		}
	})
	return err
}

func tokenUnaryInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := authorize(ctx, token); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func tokenStreamInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authorize(ss.Context(), token); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func authorize(ctx context.Context, token string) error {
	if token == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	values := md.Get("authorization")
	for _, v := range values {
		if v == "Bearer "+token {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "invalid token")
}
