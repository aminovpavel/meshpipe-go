package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/aminovpavel/meshpipe-go/internal/decode"
	"github.com/aminovpavel/meshpipe-go/internal/mqtt"
	"github.com/aminovpavel/meshpipe-go/internal/observability"
	"github.com/aminovpavel/meshpipe-go/internal/storage"
)

// Client abstracts the MQTT client behaviour required by the pipeline.
type Client interface {
	Start(ctx context.Context) error
	Stop()
	Messages() <-chan mqtt.Message
	Errors() <-chan error
}

// Pipeline wires the MQTT client with decoder and storage writer.
type Pipeline struct {
	client  Client
	decoder decode.Decoder
	writer  storage.Writer
	errCh   chan error
	wg      sync.WaitGroup

	logger           *slog.Logger
	metrics          *observability.Metrics
	maxEnvelopeBytes int
}

// Option configures optional pipeline components.
type Option func(*Pipeline)

// WithLogger injects a structured logger into the pipeline.
func WithLogger(logger *slog.Logger) Option {
	return func(p *Pipeline) {
		if logger != nil {
			p.logger = logger
		}
	}
}

// WithMetrics attaches metrics instrumentation to the pipeline.
func WithMetrics(metrics *observability.Metrics) Option {
	return func(p *Pipeline) {
		if metrics != nil {
			p.metrics = metrics
		}
	}
}

// WithMaxEnvelopeBytes sets an upper bound on allowed MQTT payload size (bytes). Zero disables the guardrail.
func WithMaxEnvelopeBytes(limit int) Option {
	return func(p *Pipeline) {
		p.maxEnvelopeBytes = limit
	}
}

// New creates a pipeline instance.
func New(client Client, decoder decode.Decoder, writer storage.Writer, opts ...Option) *Pipeline {
	p := &Pipeline{
		client:  client,
		decoder: decoder,
		writer:  writer,
		errCh:   make(chan error, 32),
		logger:  slog.Default(),
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.logger == nil {
		p.logger = slog.Default()
	}
	return p
}

// Errors exposes asynchronous processing errors.
func (p *Pipeline) Errors() <-chan error {
	return p.errCh
}

// Run starts the pipeline and blocks until the context is cancelled or the client stops.
func (p *Pipeline) Run(ctx context.Context) error {
	if p.client == nil {
		return fmt.Errorf("pipeline: client is nil")
	}
	if p.decoder == nil {
		return fmt.Errorf("pipeline: decoder is nil")
	}
	if p.writer == nil {
		return fmt.Errorf("pipeline: writer is nil")
	}

	if err := p.client.Start(ctx); err != nil {
		return fmt.Errorf("pipeline: start client: %w", err)
	}

	p.wg.Add(2)
	go p.consume(ctx)
	go p.forwardClientErrors(ctx)

	<-ctx.Done()
	p.client.Stop()
	p.wg.Wait()
	close(p.errCh)

	return nil
}

func (p *Pipeline) consume(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-p.client.Messages():
			if !ok {
				return
			}
			if p.maxEnvelopeBytes > 0 && len(msg.Payload) > p.maxEnvelopeBytes {
				p.metrics.IncDroppedMessages()
				if p.logger != nil {
					p.logger.Warn("dropping oversize MQTT payload",
						slog.Int("payload_bytes", len(msg.Payload)),
						slog.Int("limit_bytes", p.maxEnvelopeBytes),
						slog.String("topic", msg.Topic))
				}
				continue
			}
			p.metrics.IncMessagesReceived()
			pkt, err := p.decoder.Decode(ctx, msg)
			if err != nil {
				p.metrics.IncDecodeErrors()
				p.publishErr(fmt.Errorf("pipeline: decode: %w", err))
				continue
			}
			if err := p.writer.Store(ctx, pkt); err != nil {
				p.metrics.IncStoreErrors()
				p.publishErr(fmt.Errorf("pipeline: store: %w", err))
			} else {
				p.metrics.ObservePacketStored(pkt)
			}
		}
	}
}

func (p *Pipeline) forwardClientErrors(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-p.client.Errors():
			if !ok {
				return
			}
			p.publishErr(fmt.Errorf("pipeline: mqtt: %w", err))
		}
	}
}

func (p *Pipeline) publishErr(err error) {
	if err == nil {
		return
	}
	p.metrics.IncPipelineErrors()
	select {
	case p.errCh <- err:
	default:
		if p.logger != nil {
			p.logger.Error("dropping pipeline error", slog.Any("error", err))
		}
	}
}
