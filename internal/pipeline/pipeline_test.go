package pipeline_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aminovpavel/meshpipe-go/internal/decode"
	"github.com/aminovpavel/meshpipe-go/internal/mqtt"
	"github.com/aminovpavel/meshpipe-go/internal/pipeline"
)

func TestPipelineProcessesMessages(t *testing.T) {
	client := newStubClient()
	decoder := stubDecoder{}
	writer := newStubWriter()
	p := pipeline.New(client, decoder, writer)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		if err := p.Run(ctx); err != nil {
			t.Errorf("pipeline run error: %v", err)
		}
		close(done)
	}()

	<-client.started

	client.messages <- mqtt.Message{Topic: "msh/test", Payload: []byte("data")}

	select {
	case <-writer.stored:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected packet to be stored")
	}

	cancel()
	client.closeChannels()
	<-done
}

func TestPipelineForwardsClientErrors(t *testing.T) {
	client := newStubClient()
	decoder := stubDecoder{}
	writer := newStubWriter()
	p := pipeline.New(client, decoder, writer)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		if err := p.Run(ctx); err != nil {
			t.Errorf("pipeline run error: %v", err)
		}
		close(done)
	}()

	<-client.started

	errExpected := errors.New("mqtt failure")
	client.errs <- errExpected

	select {
	case err := <-p.Errors():
		if err == nil || err.Error() == "" {
			t.Fatalf("expected forwarded error, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected error to be forwarded")
	}

	cancel()
	client.closeChannels()
	<-done
}

// --- test doubles ---

type stubClient struct {
	messages chan mqtt.Message
	errs     chan error
	started  chan struct{}
	stopOnce sync.Once
}

func newStubClient() *stubClient {
	return &stubClient{
		messages: make(chan mqtt.Message, 1),
		errs:     make(chan error, 1),
		started:  make(chan struct{}),
	}
}

func (s *stubClient) Start(context.Context) error {
	s.stopOnce = sync.Once{}
	closeChan(s.started)
	return nil
}

func (s *stubClient) Stop() {
	s.closeChannels()
}

func (s *stubClient) closeChannels() {
	s.stopOnce.Do(func() {
		closeChan(s.messages)
		closeChan(s.errs)
	})
}

func (s *stubClient) Messages() <-chan mqtt.Message { return s.messages }
func (s *stubClient) Errors() <-chan error          { return s.errs }

type stubDecoder struct{}

func (stubDecoder) Decode(ctx context.Context, msg mqtt.Message) (decode.Packet, error) {
	return decode.Packet{
		Topic:      msg.Topic,
		Payload:    append([]byte(nil), msg.Payload...),
		QoS:        msg.QoS,
		Retained:   msg.Retained,
		ReceivedAt: msg.Time,
	}, nil
}

type stubWriter struct {
	stored chan decode.Packet
}

func newStubWriter() *stubWriter {
	return &stubWriter{stored: make(chan decode.Packet, 1)}
}

func (s *stubWriter) Store(ctx context.Context, pkt decode.Packet) error {
	s.stored <- pkt
	return nil
}

func closeChan[T any](ch chan T) {
	defer func() { _ = recover() }()
	close(ch)
}
