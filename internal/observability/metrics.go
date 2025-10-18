package observability

import (
	"sync/atomic"

	"github.com/aminovpavel/mw-malla-capture/internal/decode"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics bundles Prometheus metrics used across the capture service.
type Metrics struct {
	namespace string

	messagesReceived   prometheus.Counter
	decodeErrors       prometheus.Counter
	storeErrors        prometheus.Counter
	packetsStored      *prometheus.CounterVec
	queueDepth         prometheus.Gauge
	nodeUpserts        prometheus.Counter
	textMessagesStored prometheus.Counter
	positionsStored    prometheus.Counter
	telemetryStored    prometheus.Counter
	pipelineErrors     prometheus.Counter
	droppedMessages    prometheus.Counter

	healthy atomic.Bool
}

// MetricsOption customises metrics creation.
type MetricsOption func(*metricsConfig)

type metricsConfig struct {
	namespace string
	registry  prometheus.Registerer
}

// WithNamespace overrides the metric namespace (default: malla_capture).
func WithNamespace(ns string) MetricsOption {
	return func(cfg *metricsConfig) {
		if ns != "" {
			cfg.namespace = ns
		}
	}
}

// WithRegistry overrides the Prometheus registerer (useful for tests).
func WithRegistry(reg prometheus.Registerer) MetricsOption {
	return func(cfg *metricsConfig) {
		if reg != nil {
			cfg.registry = reg
		}
	}
}

// NewMetrics initialises and registers capture metrics.
func NewMetrics(opts ...MetricsOption) *Metrics {
	cfg := metricsConfig{
		namespace: "malla_capture",
		registry:  prometheus.DefaultRegisterer,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	m := &Metrics{
		namespace: cfg.namespace,
		messagesReceived: promauto.With(cfg.registry).NewCounter(prometheus.CounterOpts{
			Namespace: cfg.namespace,
			Name:      "messages_received_total",
			Help:      "Total number of MQTT messages received from the broker.",
		}),
		decodeErrors: promauto.With(cfg.registry).NewCounter(prometheus.CounterOpts{
			Namespace: cfg.namespace,
			Name:      "decode_errors_total",
			Help:      "Total number of decoding failures.",
		}),
		storeErrors: promauto.With(cfg.registry).NewCounter(prometheus.CounterOpts{
			Namespace: cfg.namespace,
			Name:      "store_errors_total",
			Help:      "Total number of storage errors.",
		}),
		packetsStored: promauto.With(cfg.registry).NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.namespace,
			Name:      "packets_stored_total",
			Help:      "Total number of packets persisted to storage, partitioned by processing status.",
		}, []string{"processed_successfully"}),
		queueDepth: promauto.With(cfg.registry).NewGauge(prometheus.GaugeOpts{
			Namespace: cfg.namespace,
			Name:      "storage_queue_depth",
			Help:      "Current number of packets waiting in the storage queue.",
		}),
		nodeUpserts: promauto.With(cfg.registry).NewCounter(prometheus.CounterOpts{
			Namespace: cfg.namespace,
			Name:      "node_upserts_total",
			Help:      "Total number of node_info rows upserted.",
		}),
		textMessagesStored: promauto.With(cfg.registry).NewCounter(prometheus.CounterOpts{
			Namespace: cfg.namespace,
			Name:      "text_messages_stored_total",
			Help:      "Total number of text message payloads stored.",
		}),
		positionsStored: promauto.With(cfg.registry).NewCounter(prometheus.CounterOpts{
			Namespace: cfg.namespace,
			Name:      "positions_stored_total",
			Help:      "Total number of position payloads stored.",
		}),
		telemetryStored: promauto.With(cfg.registry).NewCounter(prometheus.CounterOpts{
			Namespace: cfg.namespace,
			Name:      "telemetry_stored_total",
			Help:      "Total number of telemetry payloads stored.",
		}),
		pipelineErrors: promauto.With(cfg.registry).NewCounter(prometheus.CounterOpts{
			Namespace: cfg.namespace,
			Name:      "pipeline_errors_total",
			Help:      "Total number of pipeline errors forwarded to the supervisor.",
		}),
		droppedMessages: promauto.With(cfg.registry).NewCounter(prometheus.CounterOpts{
			Namespace: cfg.namespace,
			Name:      "messages_dropped_total",
			Help:      "Total number of MQTT messages dropped before decode.",
		}),
	}

	m.healthy.Store(true)
	return m
}

// IncMessagesReceived increments the raw message counter.
func (m *Metrics) IncMessagesReceived() {
	if m == nil {
		return
	}
	m.messagesReceived.Inc()
}

// IncDecodeErrors increments decode error counter and marks service unhealthy.
func (m *Metrics) IncDecodeErrors() {
	if m == nil {
		return
	}
	m.decodeErrors.Inc()
	m.healthy.Store(false)
}

// IncStoreErrors increments store error counter and marks service unhealthy.
func (m *Metrics) IncStoreErrors() {
	if m == nil {
		return
	}
	m.storeErrors.Inc()
	m.healthy.Store(false)
}

// ObservePacketStored records a packet persistence event.
func (m *Metrics) ObservePacketStored(pkt decode.Packet) {
	if m == nil {
		return
	}
	status := "false"
	if pkt.ProcessedSuccessfully {
		status = "true"
	}
	m.packetsStored.WithLabelValues(status).Inc()
}

// ObserveQueueDepth tracks the storage queue depth.
func (m *Metrics) ObserveQueueDepth(depth int) {
	if m == nil {
		return
	}
	m.queueDepth.Set(float64(depth))
}

// IncNodeUpsert notes a node_info upsert.
func (m *Metrics) IncNodeUpsert() {
	if m == nil {
		return
	}
	m.nodeUpserts.Inc()
}

// IncTextStored notes a persisted text payload.
func (m *Metrics) IncTextStored() {
	if m == nil {
		return
	}
	m.textMessagesStored.Inc()
}

// IncPositionStored notes a persisted position payload.
func (m *Metrics) IncPositionStored() {
	if m == nil {
		return
	}
	m.positionsStored.Inc()
}

// IncTelemetryStored notes a persisted telemetry payload.
func (m *Metrics) IncTelemetryStored() {
	if m == nil {
		return
	}
	m.telemetryStored.Inc()
}

// IncPipelineErrors increments general pipeline error counter.
func (m *Metrics) IncPipelineErrors() {
	if m == nil {
		return
	}
	m.pipelineErrors.Inc()
	m.healthy.Store(false)
}

func (m *Metrics) IncDroppedMessages() {
	if m == nil {
		return
	}
	m.droppedMessages.Inc()
}

// Healthy reports whether recent operations have seen errors.
func (m *Metrics) Healthy() bool {
	if m == nil {
		return true
	}
	return m.healthy.Load()
}

// MarkHealthy resets the healthy flag.
func (m *Metrics) MarkHealthy() {
	if m == nil {
		return
	}
	m.healthy.Store(true)
}
