package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/aminovpavel/mw-malla-capture/internal/decode"
	"github.com/aminovpavel/mw-malla-capture/internal/mqtt"
	"github.com/aminovpavel/mw-malla-capture/internal/storage"
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
}

// New creates a pipeline instance.
func New(client Client, decoder decode.Decoder, writer storage.Writer) *Pipeline {
	return &Pipeline{
		client:  client,
		decoder: decoder,
		writer:  writer,
		errCh:   make(chan error, 32),
	}
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
			pkt, err := p.decoder.Decode(ctx, msg)
			if err != nil {
				p.publishErr(fmt.Errorf("pipeline: decode: %w", err))
				continue
			}
			if err := p.writer.Store(ctx, pkt); err != nil {
				p.publishErr(fmt.Errorf("pipeline: store: %w", err))
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
	select {
	case p.errCh <- err:
	default:
	}
}
