package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/aminovpavel/meshpipe-go/internal/decode"
	meshtasticpb "github.com/aminovpavel/meshpipe-go/internal/decode/pb/meshtastic"
	"github.com/aminovpavel/meshpipe-go/internal/observability"
	"google.golang.org/protobuf/proto"
)

// SQLiteConfig holds configuration values for the SQLite writer.
type SQLiteConfig struct {
	Path                string
	QueueSize           int
	MaintenanceInterval time.Duration
}

// Writer is the minimal interface required by the pipeline to persist packets.
type Writer interface {
	Store(ctx context.Context, pkt decode.Packet) error
}

// StartStopper represents writers that need explicit lifecycle management.
type StartStopper interface {
	Writer
	Start(ctx context.Context) error
	Stop() error
}

// SQLiteWriter persists packets into a SQLite database.
type SQLiteWriter struct {
	cfg   SQLiteConfig
	db    *sql.DB
	queue chan decode.Packet
	wg    sync.WaitGroup
	once  sync.Once

	logger  *slog.Logger
	metrics *observability.Metrics
	cache   *nodeCache

	maintenanceInterval time.Duration
	maintenanceStop     chan struct{}
}

// NewSQLiteWriter constructs a writer with the provided configuration.
func NewSQLiteWriter(cfg SQLiteConfig, opts ...Option) (*SQLiteWriter, error) {
	if cfg.Path == "" {
		return nil, errors.New("storage: database path must be provided")
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 512
	}
	if cfg.MaintenanceInterval <= 0 {
		cfg.MaintenanceInterval = 6 * time.Hour
	}

	w := &SQLiteWriter{
		cfg:                 cfg,
		queue:               make(chan decode.Packet, cfg.QueueSize),
		logger:              slog.Default(),
		cache:               newNodeCache(),
		maintenanceInterval: cfg.MaintenanceInterval,
		maintenanceStop:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(w)
	}
	if w.logger == nil {
		w.logger = slog.Default()
	}

	return w, nil
}

// Option configures the writer.
type Option func(*SQLiteWriter)

// WithLogger injects a structured logger into the writer.
func WithLogger(logger *slog.Logger) Option {
	return func(w *SQLiteWriter) {
		if logger != nil {
			w.logger = logger
		}
	}
}

// WithMetrics attaches metrics instrumentation.
func WithMetrics(metrics *observability.Metrics) Option {
	return func(w *SQLiteWriter) {
		if metrics != nil {
			w.metrics = metrics
		}
	}
}

// Start opens the database, runs migrations, and begins processing the queue.
func (w *SQLiteWriter) Start(ctx context.Context) error {
	// Ensure directory exists.
	abs, err := filepath.Abs(w.cfg.Path)
	if err != nil {
		return fmt.Errorf("storage: resolve path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fmt.Errorf("storage: ensure directory: %w", err)
	}

	db, err := sql.Open("sqlite", abs)
	if err != nil {
		return fmt.Errorf("storage: open sqlite: %w", err)
	}

	if err := configureConnection(db); err != nil {
		db.Close()
		return err
	}

	if err := migrate(db); err != nil {
		db.Close()
		return err
	}

	if w.cache == nil {
		w.cache = newNodeCache()
	}
	if err := w.cache.load(db); err != nil {
		db.Close()
		return fmt.Errorf("storage: load node cache: %w", err)
	}

	w.db = db
	w.startMaintenance(ctx)

	w.wg.Add(1)
	go w.loop(ctx)

	return nil
}

// Store pushes a packet into the queue for asynchronous persistence.
func (w *SQLiteWriter) Store(ctx context.Context, pkt decode.Packet) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.queue <- pkt:
		w.metrics.ObserveQueueDepth(len(w.queue))
		return nil
	default:
		w.metrics.ObserveQueueDepth(len(w.queue))
		return errors.New("storage: queue full")
	}
}

// Stop finalises the writer and closes the database connection.
func (w *SQLiteWriter) Stop() error {
	w.once.Do(func() {
		if w.maintenanceStop != nil {
			close(w.maintenanceStop)
		}
		close(w.queue)
		w.wg.Wait()
		if w.db != nil {
			w.runFinalMaintenance()
			_ = w.db.Close()
		}
		w.metrics.ObserveQueueDepth(0)
	})
	return nil
}

func (w *SQLiteWriter) startMaintenance(ctx context.Context) {
	if w.maintenanceInterval <= 0 || w.db == nil {
		return
	}

	ticker := time.NewTicker(w.maintenanceInterval)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.maintenanceStop:
				return
			case <-ticker.C:
				if err := w.runMaintenance(ctx); err != nil && !errors.Is(err, context.Canceled) {
					w.logger.Warn("sqlite maintenance failed", slog.Any("error", err))
				}
			}
		}
	}()
}

func (w *SQLiteWriter) runMaintenance(ctx context.Context) error {
	if w.db == nil {
		return nil
	}

	start := time.Now()
	if _, err := w.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return context.Canceled
		}
		return fmt.Errorf("maintenance: wal_checkpoint: %w", err)
	}
	if _, err := w.db.ExecContext(ctx, "PRAGMA optimize"); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return context.Canceled
		}
		return fmt.Errorf("maintenance: optimize: %w", err)
	}
	if w.logger != nil {
		w.logger.Info("sqlite maintenance completed",
			slog.Duration("duration", time.Since(start)))
	}
	return nil
}

func (w *SQLiteWriter) runFinalMaintenance() {
	if w.db == nil {
		return
	}

	if _, err := w.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		w.logger.Warn("final maintenance checkpoint failed", slog.Any("error", err))
	}
	if _, err := w.db.Exec("VACUUM"); err != nil {
		w.logger.Warn("final maintenance vacuum failed", slog.Any("error", err))
	}
	if _, err := w.db.Exec("ANALYZE"); err != nil {
		w.logger.Warn("final maintenance analyze failed", slog.Any("error", err))
	}
}

func (w *SQLiteWriter) loop(ctx context.Context) {
	defer w.wg.Done()

	stmt, err := w.db.Prepare(`INSERT INTO packet_history (
        timestamp,
        topic,
        from_node_id,
        to_node_id,
        portnum,
        portnum_name,
        gateway_id,
        channel_id,
        channel_name,
        mesh_packet_id,
        rssi,
        snr,
        hop_limit,
        hop_start,
        payload_length,
        raw_payload,
        processed_successfully,
        via_mqtt,
        want_ack,
        priority,
        delayed,
        channel_index,
        rx_time,
        pki_encrypted,
        next_hop,
        relay_node,
        tx_after,
        message_type,
        raw_service_envelope,
        parsing_error,
        transport,
        qos,
        retained
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		w.publishErr(fmt.Errorf("storage: prepare insert: %w", err))
		return
	}
	defer stmt.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case pkt, ok := <-w.queue:
			if !ok {
				return
			}
			w.metrics.ObserveQueueDepth(len(w.queue))

			packetID, err := insertPacket(stmt, pkt)
			if err != nil {
				w.metrics.IncStoreErrors()
				w.publishErr(err)
				continue
			}

			if w.cache != nil {
				if entry, created := w.cache.ensureGateway(pkt.GatewayID, pkt.ReceivedAt); created {
					if err := w.upsertNode(entry); err != nil {
						w.metrics.IncStoreErrors()
						w.publishErr(err)
					} else {
						w.metrics.IncNodeUpsert()
					}
				}

				if nodeEntry := w.cache.updateFromPacket(pkt); nodeEntry != nil {
					if err := w.upsertNode(nodeEntry); err != nil {
						w.metrics.IncStoreErrors()
						w.publishErr(err)
					} else {
						w.metrics.IncNodeUpsert()
					}
				}
			} else if pkt.Node != nil {
				fallback := &nodeEntry{
					NodeID:         pkt.From,
					UserID:         pkt.Node.UserID,
					HexID:          pkt.Node.UserID,
					LongName:       pkt.Node.LongName,
					ShortName:      pkt.Node.ShortName,
					HWModel:        pkt.Node.HWModel,
					Role:           pkt.Node.Role,
					IsLicensed:     pkt.Node.IsLicensed,
					MacAddress:     pkt.Node.MacAddress,
					PrimaryChannel: nonEmpty(pkt.Node.PrimaryChannel, pkt.ChannelID),
					Snr:            pkt.Node.Snr,
					LastHeard:      pkt.Node.LastHeard,
					ViaMQTT:        pkt.Node.ViaMQTT,
					Channel:        pkt.Node.Channel,
					IsFavorite:     pkt.Node.IsFavorite,
					IsIgnored:      pkt.Node.IsIgnored,
					IsKeyVerified:  pkt.Node.IsKeyVerified,
					FirstSeen:      pkt.ReceivedAt,
					LastUpdated:    pkt.ReceivedAt,
				}
				if pkt.Node.HopsAway != nil {
					val := *pkt.Node.HopsAway
					fallback.HopsAway = &val
				}
				if err := w.upsertNode(fallback); err != nil {
					w.metrics.IncStoreErrors()
					w.publishErr(err)
				} else {
					w.metrics.IncNodeUpsert()
				}
			}

			if pkt.Text != nil {
				if err := w.storeText(packetID, pkt.Text); err != nil {
					w.metrics.IncStoreErrors()
					w.publishErr(err)
				} else {
					w.metrics.IncTextStored()
				}
			}

			if pkt.Position != nil {
				if err := w.storePosition(packetID, pkt.Position); err != nil {
					w.metrics.IncStoreErrors()
					w.publishErr(err)
				} else {
					w.metrics.IncPositionStored()
				}
			}

			if pkt.Telemetry != nil {
				if err := w.storeTelemetry(packetID, pkt.Telemetry); err != nil {
					w.metrics.IncStoreErrors()
					w.publishErr(err)
				} else {
					w.metrics.IncTelemetryStored()
				}
			}

			if err := w.storeRangeTest(packetID, pkt); err != nil {
				w.metrics.IncStoreErrors()
				w.publishErr(err)
			}

			if err := w.storeStoreForward(packetID, pkt); err != nil {
				w.metrics.IncStoreErrors()
				w.publishErr(err)
			}

			if err := w.storePaxcounter(packetID, pkt); err != nil {
				w.metrics.IncStoreErrors()
				w.publishErr(err)
			}

			if err := w.storeTraceroute(packetID, pkt); err != nil {
				w.metrics.IncStoreErrors()
				w.publishErr(err)
			}

			if err := w.storeLinkHistory(packetID, pkt); err != nil {
				w.metrics.IncStoreErrors()
				w.publishErr(err)
			}

			if err := w.upsertGatewayNodeStats(pkt); err != nil {
				w.metrics.IncStoreErrors()
				w.publishErr(err)
			}

			if err := w.storeNeighborHistory(packetID, pkt); err != nil {
				w.metrics.IncStoreErrors()
				w.publishErr(err)
			}
		}
	}
}

func insertPacket(stmt *sql.Stmt, pkt decode.Packet) (int64, error) {
	res, err := stmt.Exec(
		timeToSeconds(pkt.ReceivedAt),
		pkt.Topic,
		int64(pkt.From),
		int64(pkt.To),
		int64(pkt.PortNum),
		pkt.PortNumName,
		nullString(pkt.GatewayID),
		nullString(pkt.ChannelID),
		nullString(pkt.ChannelName),
		int64(pkt.MeshPacketID),
		int64(pkt.RxRssi),
		float64(pkt.RxSnr),
		int64(pkt.HopLimit),
		int64(pkt.HopStart),
		pkt.PayloadLength,
		nullBytes(pkt.Payload),
		boolToInt(pkt.ProcessedSuccessfully),
		boolToInt(pkt.ViaMQTT),
		boolToInt(pkt.WantAck),
		int64(pkt.Priority),
		int64(pkt.Delayed),
		int64(pkt.ChannelIndex),
		int64(pkt.RxTime),
		boolToInt(pkt.PKIEncrypted),
		int64(pkt.NextHop),
		int64(pkt.RelayNode),
		int64(pkt.TxAfter),
		nullString(pkt.MessageType),
		nullBytes(pkt.RawServiceEnvelope),
		nullString(pkt.ParsingError),
		int64(pkt.Transport),
		int64(pkt.QoS),
		boolToInt(pkt.Retained),
	)
	if err != nil {
		return 0, fmt.Errorf("storage: insert packet: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("storage: last insert id: %w", err)
	}
	return id, nil
}

func (w *SQLiteWriter) storeText(packetID int64, msg *decode.TextMessage) error {
	_, err := w.db.Exec(`INSERT INTO text_messages (
        packet_id,
        text,
        want_response,
        dest,
        source,
        request_id,
        reply_id,
        emoji,
        bitfield,
        compressed
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		packetID,
		msg.Text,
		boolToInt(msg.WantResponse),
		int64(msg.Dest),
		int64(msg.Source),
		int64(msg.RequestID),
		int64(msg.ReplyID),
		int64(msg.Emoji),
		int64(msg.Bitfield),
		boolToInt(msg.Compressed),
	)
	if err != nil {
		return fmt.Errorf("storage: insert text message: %w", err)
	}
	return nil
}

func (w *SQLiteWriter) storePosition(packetID int64, pos *decode.PositionInfo) error {
	_, err := w.db.Exec(`INSERT INTO positions (
        packet_id,
        latitude,
        longitude,
        altitude,
        time,
        timestamp,
        raw_payload
    ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		packetID,
		nullFloat64(pos.Latitude),
		nullFloat64(pos.Longitude),
		nullInt32(pos.Altitude),
		int64(pos.Time),
		int64(pos.Timestamp),
		nullBytes(pos.RawPayload),
	)
	if err != nil {
		return fmt.Errorf("storage: insert position: %w", err)
	}
	return nil
}

func (w *SQLiteWriter) storeTelemetry(packetID int64, tele *decode.TelemetryInfo) error {
	_, err := w.db.Exec(`INSERT INTO telemetry (
        packet_id,
        raw_payload
    ) VALUES (?, ?)`,
		packetID,
		nullBytes(tele.RawPayload),
	)
	if err != nil {
		return fmt.Errorf("storage: insert telemetry: %w", err)
	}
	return nil
}

func (w *SQLiteWriter) storeRangeTest(packetID int64, pkt decode.Packet) error {
	if len(pkt.ExtraText) == 0 {
		return nil
	}
	text, ok := pkt.ExtraText[meshtasticpb.PortNum_RANGE_TEST_APP.String()]
	if !ok || strings.TrimSpace(text) == "" {
		return nil
	}
	_, err := w.db.Exec(`INSERT INTO range_test_results (
	        packet_id,
	        text,
	        raw_payload
	    ) VALUES (?, ?, ?)
	    ON CONFLICT(packet_id) DO UPDATE SET
	        text=excluded.text,
	        raw_payload=excluded.raw_payload`,
		packetID,
		text,
		nullBytes(pkt.Payload),
	)
	if err != nil {
		return fmt.Errorf("storage: insert range_test: %w", err)
	}
	return nil
}

func (w *SQLiteWriter) storeStoreForward(packetID int64, pkt decode.Packet) error {
	if len(pkt.DecodedPortPayload) == 0 {
		return nil
	}
	msg, ok := pkt.DecodedPortPayload[meshtasticpb.PortNum_STORE_FORWARD_APP.String()]
	if !ok || msg == nil {
		return nil
	}
	store, ok := msg.(*meshtasticpb.StoreAndForward)
	if !ok || store == nil {
		return nil
	}
	variant := "none"
	stats := store.GetStats()
	history := store.GetHistory()
	heartbeat := store.GetHeartbeat()
	textPayload := store.GetText()
	if stats != nil {
		variant = "stats"
	} else if history != nil {
		variant = "history"
	} else if heartbeat != nil {
		variant = "heartbeat"
	} else if len(textPayload) > 0 {
		variant = "text"
	}
	raw, err := proto.Marshal(store)
	if err != nil {
		return fmt.Errorf("storage: marshal store forward: %w", err)
	}
	_, err = w.db.Exec(`INSERT INTO store_forward_events (
	        packet_id,
	        request_response,
	        variant,
	        messages_total,
	        messages_saved,
	        messages_max,
	        uptime_seconds,
	        requests_total,
	        requests_history,
	        heartbeat_flag,
	        return_max,
	        return_window,
	        history_messages,
	        history_window,
	        history_last_request,
	        heartbeat_period,
	        heartbeat_secondary,
	        text_payload,
	        raw_payload
	    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	    ON CONFLICT(packet_id) DO UPDATE SET
	        request_response=excluded.request_response,
	        variant=excluded.variant,
	        messages_total=excluded.messages_total,
	        messages_saved=excluded.messages_saved,
	        messages_max=excluded.messages_max,
	        uptime_seconds=excluded.uptime_seconds,
	        requests_total=excluded.requests_total,
	        requests_history=excluded.requests_history,
	        heartbeat_flag=excluded.heartbeat_flag,
	        return_max=excluded.return_max,
	        return_window=excluded.return_window,
	        history_messages=excluded.history_messages,
	        history_window=excluded.history_window,
	        history_last_request=excluded.history_last_request,
	        heartbeat_period=excluded.heartbeat_period,
	        heartbeat_secondary=excluded.heartbeat_secondary,
	        text_payload=excluded.text_payload,
	        raw_payload=excluded.raw_payload`,
		packetID,
		store.GetRr().String(),
		variant,
		nullUint32(statsSafe(stats, func(s *meshtasticpb.StoreAndForward_Statistics) uint32 { return s.GetMessagesTotal() })),
		nullUint32(statsSafe(stats, func(s *meshtasticpb.StoreAndForward_Statistics) uint32 { return s.GetMessagesSaved() })),
		nullUint32(statsSafe(stats, func(s *meshtasticpb.StoreAndForward_Statistics) uint32 { return s.GetMessagesMax() })),
		nullUint32(statsSafe(stats, func(s *meshtasticpb.StoreAndForward_Statistics) uint32 { return s.GetUpTime() })),
		nullUint32(statsSafe(stats, func(s *meshtasticpb.StoreAndForward_Statistics) uint32 { return s.GetRequests() })),
		nullUint32(statsSafe(stats, func(s *meshtasticpb.StoreAndForward_Statistics) uint32 { return s.GetRequestsHistory() })),
		boolToInt(stats != nil && stats.GetHeartbeat()),
		nullUint32(statsSafe(stats, func(s *meshtasticpb.StoreAndForward_Statistics) uint32 { return s.GetReturnMax() })),
		nullUint32(statsSafe(stats, func(s *meshtasticpb.StoreAndForward_Statistics) uint32 { return s.GetReturnWindow() })),
		nullUint32(historySafe(history, func(h *meshtasticpb.StoreAndForward_History) uint32 { return h.GetHistoryMessages() })),
		nullUint32(historySafe(history, func(h *meshtasticpb.StoreAndForward_History) uint32 { return h.GetWindow() })),
		nullUint32(historySafe(history, func(h *meshtasticpb.StoreAndForward_History) uint32 { return h.GetLastRequest() })),
		nullUint32(heartbeatSafe(heartbeat, func(h *meshtasticpb.StoreAndForward_Heartbeat) uint32 { return h.GetPeriod() })),
		nullUint32(heartbeatSafe(heartbeat, func(h *meshtasticpb.StoreAndForward_Heartbeat) uint32 { return h.GetSecondary() })),
		nullBytes(textPayload),
		nullBytes(raw),
	)
	if err != nil {
		return fmt.Errorf("storage: insert store_forward: %w", err)
	}
	return nil
}

func (w *SQLiteWriter) storePaxcounter(packetID int64, pkt decode.Packet) error {
	if len(pkt.DecodedPortPayload) == 0 {
		return nil
	}
	msg, ok := pkt.DecodedPortPayload[meshtasticpb.PortNum_PAXCOUNTER_APP.String()]
	if !ok || msg == nil {
		return nil
	}
	pax, ok := msg.(*meshtasticpb.Paxcount)
	if !ok || pax == nil {
		return nil
	}
	raw, err := proto.Marshal(pax)
	if err != nil {
		return fmt.Errorf("storage: marshal paxcounter: %w", err)
	}
	_, err = w.db.Exec(`INSERT INTO paxcounter_samples (
	        packet_id,
	        wifi,
	        ble,
	        uptime_seconds,
	        raw_payload
	    ) VALUES (?, ?, ?, ?, ?)
	    ON CONFLICT(packet_id) DO UPDATE SET
	        wifi=excluded.wifi,
	        ble=excluded.ble,
	        uptime_seconds=excluded.uptime_seconds,
	        raw_payload=excluded.raw_payload`,
		packetID,
		nullUint32(uint32Ptr(pax.GetWifi())),
		nullUint32(uint32Ptr(pax.GetBle())),
		nullUint32(uint32Ptr(pax.GetUptime())),
		nullBytes(raw),
	)
	if err != nil {
		return fmt.Errorf("storage: insert paxcounter: %w", err)
	}
	return nil
}

func (w *SQLiteWriter) storeTraceroute(packetID int64, pkt decode.Packet) error {
	if len(pkt.Traceroutes) == 0 {
		return nil
	}
	gatewayID := strings.TrimSpace(pkt.GatewayID)
	receivedAt := timeToSeconds(pkt.ReceivedAt)
	requestID := int64(pkt.RequestID)
	originID := int64(pkt.From)
	destID := int64(pkt.To)

	insertHop := func(direction string, hopIndex int, hopNode uint32, snrVal interface{}, origin, dest int64) error {
		_, err := w.db.Exec(`INSERT INTO traceroute_hops (
	            packet_id,
	            gateway_id,
	            request_id,
	            origin_node_id,
	            destination_node_id,
	            direction,
	            hop_index,
	            hop_node_id,
	            snr,
	            received_at
	        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			packetID,
			nullString(gatewayID),
			requestID,
			origin,
			dest,
			direction,
			hopIndex,
			int64(hopNode),
			snrVal,
			receivedAt,
		)
		if err != nil {
			return fmt.Errorf("storage: insert traceroute hop: %w", err)
		}
		return nil
	}

	for _, tr := range pkt.Traceroutes {
		if tr == nil {
			continue
		}
		route := tr.GetRoute()
		snrTowards := tr.GetSnrTowards()
		for idx, hop := range route {
			if hop == 0 {
				continue
			}
			if err := insertHop("towards", idx, hop, nullFloat64(snrPointer(snrTowards, idx)), originID, destID); err != nil {
				return err
			}
		}

		routeBack := tr.GetRouteBack()
		snrBack := tr.GetSnrBack()
		for idx, hop := range routeBack {
			if hop == 0 {
				continue
			}
			if err := insertHop("back", idx, hop, nullFloat64(snrPointer(snrBack, idx)), destID, originID); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *SQLiteWriter) storeLinkHistory(packetID int64, pkt decode.Packet) error {
	gatewayID := strings.TrimSpace(pkt.GatewayID)
	if gatewayID == "" {
		return nil
	}
	_, err := w.db.Exec(`INSERT INTO link_history (
	        packet_id,
	        gateway_id,
	        from_node_id,
	        to_node_id,
	        hop_index,
	        hop_limit,
	        rssi,
	        snr,
	        channel_id,
	        channel_name,
	        received_at
	    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	    ON CONFLICT(packet_id) DO UPDATE SET
	        gateway_id=excluded.gateway_id,
	        from_node_id=excluded.from_node_id,
	        to_node_id=excluded.to_node_id,
	        hop_index=excluded.hop_index,
	        hop_limit=excluded.hop_limit,
	        rssi=excluded.rssi,
	        snr=excluded.snr,
	        channel_id=excluded.channel_id,
	        channel_name=excluded.channel_name,
	        received_at=excluded.received_at`,
		packetID,
		gatewayID,
		int64(pkt.From),
		int64(pkt.To),
		int64(pkt.HopStart),
		int64(pkt.HopLimit),
		int64(pkt.RxRssi),
		float64(pkt.RxSnr),
		nullString(pkt.ChannelID),
		nullString(pkt.ChannelName),
		timeToSeconds(pkt.ReceivedAt),
	)
	if err != nil {
		return fmt.Errorf("storage: insert link_history: %w", err)
	}
	return nil
}

func (w *SQLiteWriter) upsertGatewayNodeStats(pkt decode.Packet) error {
	gatewayID := strings.TrimSpace(pkt.GatewayID)
	if gatewayID == "" {
		return nil
	}
	if pkt.From == 0 {
		return nil
	}
	_, err := w.db.Exec(`INSERT INTO gateway_node_stats (
	        gateway_id,
	        node_id,
	        first_seen,
	        last_seen,
	        packets_total,
	        last_rssi,
	        last_snr
	    ) VALUES (?, ?, ?, ?, 1, ?, ?)
	    ON CONFLICT(gateway_id, node_id) DO UPDATE SET
	        first_seen=MIN(gateway_node_stats.first_seen, excluded.first_seen),
	        last_seen=MAX(gateway_node_stats.last_seen, excluded.last_seen),
	        packets_total=gateway_node_stats.packets_total + 1,
	        last_rssi=excluded.last_rssi,
	        last_snr=excluded.last_snr`,
		gatewayID,
		int64(pkt.From),
		timeToSeconds(pkt.ReceivedAt),
		timeToSeconds(pkt.ReceivedAt),
		int64(pkt.RxRssi),
		float64(pkt.RxSnr),
	)
	if err != nil {
		return fmt.Errorf("storage: upsert gateway_node_stats: %w", err)
	}
	return nil
}

func (w *SQLiteWriter) storeNeighborHistory(packetID int64, pkt decode.Packet) error {
	msg, ok := pkt.DecodedPortPayload[meshtasticpb.PortNum_NEIGHBORINFO_APP.String()]
	if !ok {
		return nil
	}
	info, ok := msg.(*meshtasticpb.NeighborInfo)
	if !ok || info == nil {
		return nil
	}
	origin := info.GetNodeId()
	if origin == 0 {
		return nil
	}
	defaultInterval := info.GetNodeBroadcastIntervalSecs()
	gatewayID := strings.TrimSpace(pkt.GatewayID)
	for _, neighbor := range info.GetNeighbors() {
		if neighbor == nil {
			continue
		}
		interval := neighbor.GetNodeBroadcastIntervalSecs()
		if interval == 0 {
			interval = defaultInterval
		}
		_, err := w.db.Exec(`INSERT INTO neighbor_history (
	            packet_id,
	            origin_node_id,
	            neighbor_node_id,
	            snr,
	            last_rx_time,
	            broadcast_interval,
	            gateway_id,
	            channel_id,
	            received_at
	        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			packetID,
			int64(origin),
			int64(neighbor.GetNodeId()),
			float64(neighbor.GetSnr()),
			int64(neighbor.GetLastRxTime()),
			int64(interval),
			nullString(gatewayID),
			nullString(pkt.ChannelID),
			timeToSeconds(pkt.ReceivedAt),
		)
		if err != nil {
			return fmt.Errorf("storage: insert neighbor_history: %w", err)
		}
	}
	return nil
}

func (w *SQLiteWriter) upsertNode(entry *nodeEntry) error {
	if entry == nil {
		return nil
	}

	if entry.HexID == "" {
		entry.HexID = entry.UserID
	}

	_, err := w.db.Exec(`INSERT INTO node_info (
	    node_id,
	    user_id,
	    hex_id,
	    long_name,
	    short_name,
	    hw_model,
	    hw_model_name,
	    role,
	    role_name,
	    is_licensed,
	    mac_address,
	    primary_channel,
	    snr,
	    last_heard,
	    via_mqtt,
	    channel,
	    hops_away,
	    is_favorite,
	    is_ignored,
	    is_key_verified,
	    first_seen,
	    last_updated,
	    region,
	    region_name,
	    firmware_version,
	    modem_preset,
	    modem_preset_name
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(node_id) DO UPDATE SET
	    user_id=excluded.user_id,
	    hex_id=excluded.hex_id,
	    long_name=excluded.long_name,
	    short_name=excluded.short_name,
	    hw_model=excluded.hw_model,
	    hw_model_name=excluded.hw_model_name,
	    role=excluded.role,
	    role_name=excluded.role_name,
	    is_licensed=excluded.is_licensed,
	    mac_address=excluded.mac_address,
	    primary_channel=excluded.primary_channel,
	    snr=excluded.snr,
	    last_heard=excluded.last_heard,
	    via_mqtt=excluded.via_mqtt,
	    channel=excluded.channel,
	    hops_away=excluded.hops_away,
	    is_favorite=excluded.is_favorite,
	    is_ignored=excluded.is_ignored,
	    is_key_verified=excluded.is_key_verified,
	    first_seen=MIN(node_info.first_seen, excluded.first_seen),
	    last_updated=excluded.last_updated,
	    region=excluded.region,
	    region_name=excluded.region_name,
	    firmware_version=excluded.firmware_version,
	    modem_preset=excluded.modem_preset,
	    modem_preset_name=excluded.modem_preset_name`,
		int64(entry.NodeID),
		nullString(entry.UserID),
		nullString(entry.HexID),
		nullString(entry.LongName),
		nullString(entry.ShortName),
		int64(entry.HWModel),
		nullString(entry.HWModelName),
		int64(entry.Role),
		nullString(entry.RoleName),
		boolToInt(entry.IsLicensed),
		nullString(entry.MacAddress),
		nullString(entry.PrimaryChannel),
		float64(entry.Snr),
		int64(entry.LastHeard),
		boolToInt(entry.ViaMQTT),
		int64(entry.Channel),
		nullUint32(entry.HopsAway),
		boolToInt(entry.IsFavorite),
		boolToInt(entry.IsIgnored),
		boolToInt(entry.IsKeyVerified),
		timeToSeconds(entry.FirstSeen),
		timeToSeconds(entry.LastUpdated),
		nullString(entry.Region),
		nullString(entry.RegionName),
		nullString(entry.FirmwareVersion),
		nullString(entry.ModemPreset),
		nullString(entry.ModemPresetName),
	)
	if err != nil {
		return fmt.Errorf("storage: upsert node: %w", err)
	}
	return nil
}

func configureConnection(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=30000",
		"PRAGMA foreign_keys=ON",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA wal_autocheckpoint=1000",
		"PRAGMA journal_size_limit=67108864",
		"PRAGMA cache_size=-8192",
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("storage: apply pragma %q: %w", pragma, err)
		}
	}

	return nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS packet_history (
	        id INTEGER PRIMARY KEY AUTOINCREMENT,
	        timestamp REAL NOT NULL,
        topic TEXT NOT NULL,
        from_node_id INTEGER,
        to_node_id INTEGER,
        portnum INTEGER,
        portnum_name TEXT,
        gateway_id TEXT,
        channel_id TEXT,
        channel_name TEXT,
        mesh_packet_id INTEGER,
        rssi INTEGER,
        snr REAL,
        hop_limit INTEGER,
        hop_start INTEGER,
        payload_length INTEGER,
        raw_payload BLOB,
        processed_successfully INTEGER,
        via_mqtt INTEGER,
        want_ack INTEGER,
        priority INTEGER,
        delayed INTEGER,
        channel_index INTEGER,
        rx_time INTEGER,
        pki_encrypted INTEGER,
        next_hop INTEGER,
        relay_node INTEGER,
        tx_after INTEGER,
        message_type TEXT,
        raw_service_envelope BLOB,
        parsing_error TEXT,
        transport INTEGER,
        qos INTEGER,
        retained INTEGER
    )`)
	if err != nil {
		return fmt.Errorf("storage: migrate packet_history: %w", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_packet_history_timestamp ON packet_history(timestamp DESC, id DESC)`); err != nil {
		return fmt.Errorf("storage: create packet_history timestamp index: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_packet_history_gateway_timestamp ON packet_history(gateway_id, timestamp DESC)`); err != nil {
		return fmt.Errorf("storage: create packet_history gateway index: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_packet_history_portnum_timestamp ON packet_history(portnum_name, timestamp DESC)`); err != nil {
		return fmt.Errorf("storage: create packet_history portnum index: %w", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS node_info (
	 	    node_id INTEGER PRIMARY KEY,
	    user_id TEXT,
	    hex_id TEXT,
	    long_name TEXT,
	    short_name TEXT,
	    hw_model INTEGER,
	    hw_model_name TEXT,
	    role INTEGER,
	    role_name TEXT,
	    is_licensed INTEGER,
	    mac_address TEXT,
	    primary_channel TEXT,
	    snr REAL,
	    last_heard INTEGER,
	    via_mqtt INTEGER,
	    channel INTEGER,
	    hops_away INTEGER,
	    is_favorite INTEGER,
	    is_ignored INTEGER,
	    is_key_verified INTEGER,
	    first_seen REAL,
	    last_updated REAL,
	    region TEXT,
	    region_name TEXT,
	    firmware_version TEXT,
	    modem_preset TEXT,
	    modem_preset_name TEXT
	 )`)
	if err != nil {
		return fmt.Errorf("storage: migrate node_info: %w", err)
	}

	if err := renameColumnIfExists(db, "node_info", "updated_at", "last_updated"); err != nil {
		return fmt.Errorf("storage: rename updated_at column: %w", err)
	}

	if err := addColumnIfMissing(db, "node_info", "hex_id", "TEXT"); err != nil {
		return fmt.Errorf("storage: add hex_id column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "primary_channel", "TEXT"); err != nil {
		return fmt.Errorf("storage: add primary_channel column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "first_seen", "REAL"); err != nil {
		return fmt.Errorf("storage: add first_seen column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "last_updated", "REAL"); err != nil {
		return fmt.Errorf("storage: add last_updated column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "hw_model_name", "TEXT"); err != nil {
		return fmt.Errorf("storage: add hw_model_name column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "role_name", "TEXT"); err != nil {
		return fmt.Errorf("storage: add role_name column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "region", "TEXT"); err != nil {
		return fmt.Errorf("storage: add region column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "region_name", "TEXT"); err != nil {
		return fmt.Errorf("storage: add region_name column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "firmware_version", "TEXT"); err != nil {
		return fmt.Errorf("storage: add firmware_version column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "modem_preset", "TEXT"); err != nil {
		return fmt.Errorf("storage: add modem_preset column: %w", err)
	}
	if err := addColumnIfMissing(db, "node_info", "modem_preset_name", "TEXT"); err != nil {
		return fmt.Errorf("storage: add modem_preset_name column: %w", err)
	}

	if err := copyColumnIfExists(db, "node_info", "updated_at", "last_updated"); err != nil {
		return fmt.Errorf("storage: copy updated_at to last_updated: %w", err)
	}

	if err := populateHexColumn(db); err != nil {
		return fmt.Errorf("storage: populate hex column: %w", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_node_hex_id ON node_info(hex_id)`); err != nil {
		return fmt.Errorf("storage: create hex_id index: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_node_primary_channel ON node_info(primary_channel)`); err != nil {
		return fmt.Errorf("storage: create primary_channel index: %w", err)
	}

	if err := ensurePacketHistoryTimestamp(db); err != nil {
		return err
	}

	if err := createRangeTestResultsTable(db); err != nil {
		return err
	}

	if err := createStoreForwardEventsTable(db); err != nil {
		return err
	}

	if err := createPaxcounterSamplesTable(db); err != nil {
		return err
	}

	if err := createTracerouteHopsTable(db); err != nil {
		return err
	}

	if err := createLinkHistoryTable(db); err != nil {
		return err
	}

	if err := createGatewayNodeStatsTable(db); err != nil {
		return err
	}

	if err := createNeighborHistoryTable(db); err != nil {
		return err
	}

	if err := createGatewayStatsView(db); err != nil {
		return err
	}

	if err := createLinkAggregateView(db); err != nil {
		return err
	}

	if err := createGatewayDiversityView(db); err != nil {
		return err
	}

	if err := createLongestLinksView(db); err != nil {
		return err
	}

	if err := createTracerouteViews(db); err != nil {
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS text_messages (
	    packet_id INTEGER PRIMARY KEY,
	    text TEXT,
	    want_response INTEGER,
	    dest INTEGER,
	    source INTEGER,
	    request_id INTEGER,
	    reply_id INTEGER,
	    emoji INTEGER,
	    bitfield INTEGER,
	    compressed INTEGER,
	    FOREIGN KEY(packet_id) REFERENCES packet_history(id) ON DELETE CASCADE
	)`)
	if err != nil {
		return fmt.Errorf("storage: migrate text_messages: %w", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS positions (
	    packet_id INTEGER PRIMARY KEY,
	    latitude REAL,
	    longitude REAL,
	    altitude INTEGER,
	    time INTEGER,
	    timestamp INTEGER,
	    raw_payload BLOB,
	    FOREIGN KEY(packet_id) REFERENCES packet_history(id) ON DELETE CASCADE
	)`)
	if err != nil {
		return fmt.Errorf("storage: migrate positions: %w", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS telemetry (
	    packet_id INTEGER PRIMARY KEY,
	    raw_payload BLOB,
	    FOREIGN KEY(packet_id) REFERENCES packet_history(id) ON DELETE CASCADE
	)`)
	if err != nil {
		return fmt.Errorf("storage: migrate telemetry: %w", err)
	}

	return nil
}

func addColumnIfMissing(db *sql.DB, table, column, columnType string) error {
	query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, columnType)
	if _, err := db.Exec(query); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "duplicate column name") {
			return nil
		}
		return err
	}
	return nil
}

func renameColumnIfExists(db *sql.DB, table, oldName, newName string) error {
	query := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", table, oldName, newName)
	if _, err := db.Exec(query); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "no such column") || strings.Contains(errMsg, "duplicate column name") || strings.Contains(errMsg, "syntax error") {
			return nil
		}
		return err
	}
	return nil
}

func copyColumnIfExists(db *sql.DB, table, from, to string) error {
	hasFrom, err := columnExists(db, table, from)
	if err != nil {
		return err
	}
	hasTo, err := columnExists(db, table, to)
	if err != nil {
		return err
	}
	if !hasFrom || !hasTo {
		return nil
	}
	query := fmt.Sprintf("UPDATE %s SET %s = COALESCE(%s, %s) WHERE %s IS NOT NULL", table, to, to, from, from)
	if _, err := db.Exec(query); err != nil {
		return err
	}
	return nil
}

func populateHexColumn(db *sql.DB) error {
	_, err := db.Exec(`UPDATE node_info SET hex_id = user_id WHERE (hex_id IS NULL OR hex_id = '') AND user_id IS NOT NULL`)
	return err
}

func createRangeTestResultsTable(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS range_test_results (
	        packet_id INTEGER PRIMARY KEY,
	        text TEXT,
	        raw_payload BLOB,
	        FOREIGN KEY(packet_id) REFERENCES packet_history(id) ON DELETE CASCADE
	    )`); err != nil {
		return fmt.Errorf("storage: create range_test_results: %w", err)
	}
	return nil
}

func createStoreForwardEventsTable(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS store_forward_events (
	        packet_id INTEGER PRIMARY KEY,
	        request_response TEXT,
	        variant TEXT,
	        messages_total INTEGER,
	        messages_saved INTEGER,
	        messages_max INTEGER,
	        uptime_seconds INTEGER,
	        requests_total INTEGER,
	        requests_history INTEGER,
	        heartbeat_flag INTEGER,
	        return_max INTEGER,
	        return_window INTEGER,
	        history_messages INTEGER,
	        history_window INTEGER,
	        history_last_request INTEGER,
	        heartbeat_period INTEGER,
	        heartbeat_secondary INTEGER,
	        text_payload BLOB,
	        raw_payload BLOB,
	        FOREIGN KEY(packet_id) REFERENCES packet_history(id) ON DELETE CASCADE
	    )`); err != nil {
		return fmt.Errorf("storage: create store_forward_events: %w", err)
	}
	return nil
}

func createPaxcounterSamplesTable(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS paxcounter_samples (
	        packet_id INTEGER PRIMARY KEY,
	        wifi INTEGER,
	        ble INTEGER,
	        uptime_seconds INTEGER,
	        raw_payload BLOB,
	        FOREIGN KEY(packet_id) REFERENCES packet_history(id) ON DELETE CASCADE
	    )`); err != nil {
		return fmt.Errorf("storage: create paxcounter_samples: %w", err)
	}
	return nil
}

func createTracerouteHopsTable(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS traceroute_hops (
	        id INTEGER PRIMARY KEY AUTOINCREMENT,
	        packet_id INTEGER,
	        gateway_id TEXT,
	        request_id INTEGER,
	        origin_node_id INTEGER,
	        destination_node_id INTEGER,
	        direction TEXT,
	        hop_index INTEGER,
	        hop_node_id INTEGER,
	        snr REAL,
	        received_at REAL,
	        FOREIGN KEY(packet_id) REFERENCES packet_history(id) ON DELETE CASCADE
	    )`); err != nil {
		return fmt.Errorf("storage: create traceroute_hops: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_traceroute_packet ON traceroute_hops(packet_id)`); err != nil {
		return fmt.Errorf("storage: index traceroute_hops packet: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_traceroute_origin_dest ON traceroute_hops(origin_node_id, destination_node_id)`); err != nil {
		return fmt.Errorf("storage: index traceroute_hops origin/dest: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_traceroute_gateway ON traceroute_hops(gateway_id)`); err != nil {
		return fmt.Errorf("storage: index traceroute_hops gateway: %w", err)
	}
	return nil
}

func createLinkHistoryTable(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS link_history (
	        id INTEGER PRIMARY KEY AUTOINCREMENT,
	        packet_id INTEGER UNIQUE,
	        gateway_id TEXT NOT NULL,
	        from_node_id INTEGER,
	        to_node_id INTEGER,
	        hop_index INTEGER,
	        hop_limit INTEGER,
	        rssi INTEGER,
	        snr REAL,
	        channel_id TEXT,
	        channel_name TEXT,
	        received_at REAL,
	        FOREIGN KEY(packet_id) REFERENCES packet_history(id) ON DELETE CASCADE
	    )`); err != nil {
		return fmt.Errorf("storage: create link_history: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_link_history_gateway ON link_history(gateway_id, received_at)`); err != nil {
		return fmt.Errorf("storage: index link_history gateway: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_link_history_nodes ON link_history(from_node_id, to_node_id)`); err != nil {
		return fmt.Errorf("storage: index link_history nodes: %w", err)
	}
	return nil
}

func createGatewayNodeStatsTable(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS gateway_node_stats (
	        gateway_id TEXT NOT NULL,
	        node_id INTEGER NOT NULL,
	        first_seen REAL,
	        last_seen REAL,
	        packets_total INTEGER NOT NULL DEFAULT 0,
	        last_rssi INTEGER,
	        last_snr REAL,
	        PRIMARY KEY (gateway_id, node_id)
	    )`); err != nil {
		return fmt.Errorf("storage: create gateway_node_stats: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_gateway_node_stats_last_seen ON gateway_node_stats(last_seen)`); err != nil {
		return fmt.Errorf("storage: index gateway_node_stats: %w", err)
	}
	return nil
}

func createNeighborHistoryTable(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS neighbor_history (
	        id INTEGER PRIMARY KEY AUTOINCREMENT,
	        packet_id INTEGER,
	        origin_node_id INTEGER,
	        neighbor_node_id INTEGER,
	        snr REAL,
	        last_rx_time INTEGER,
	        broadcast_interval INTEGER,
	        gateway_id TEXT,
	        channel_id TEXT,
	        received_at REAL,
	        FOREIGN KEY(packet_id) REFERENCES packet_history(id) ON DELETE CASCADE
	    )`); err != nil {
		return fmt.Errorf("storage: create neighbor_history: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_neighbor_history_origin ON neighbor_history(origin_node_id)`); err != nil {
		return fmt.Errorf("storage: index neighbor_history origin: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_neighbor_history_packet ON neighbor_history(packet_id)`); err != nil {
		return fmt.Errorf("storage: index neighbor_history packet: %w", err)
	}
	return nil
}

func createGatewayStatsView(db *sql.DB) error {
	if _, err := db.Exec(`DROP VIEW IF EXISTS gateway_stats`); err != nil {
		return fmt.Errorf("storage: drop gateway_stats view: %w", err)
	}
	if _, err := db.Exec(`CREATE VIEW gateway_stats AS
	        SELECT
	            gateway_id,
	            SUM(packets_total) AS packets_total,
	            COUNT(*) AS distinct_nodes,
	            MIN(first_seen) AS first_seen,
	            MAX(last_seen) AS last_seen,
	            AVG(last_rssi) AS avg_rssi,
	            AVG(last_snr) AS avg_snr
	        FROM gateway_node_stats
	        GROUP BY gateway_id`); err != nil {
		return fmt.Errorf("storage: create gateway_stats view: %w", err)
	}
	return nil
}

func createLinkAggregateView(db *sql.DB) error {
	if _, err := db.Exec(`DROP VIEW IF EXISTS link_aggregate`); err != nil {
		return fmt.Errorf("storage: drop link_aggregate view: %w", err)
	}
	if _, err := db.Exec(`CREATE VIEW link_aggregate AS
	        SELECT
	            gateway_id,
	            channel_id,
	            from_node_id,
	            to_node_id,
	            COUNT(*) AS packets_total,
	            MIN(received_at) AS first_seen,
	            MAX(received_at) AS last_seen,
	            AVG(rssi) AS avg_rssi,
	            AVG(snr) AS avg_snr,
	            MAX(hop_index) AS max_hop_index,
	            MAX(hop_limit) AS max_hop_limit,
	            MAX(packet_id) AS last_packet_id
	        FROM link_history
	        GROUP BY gateway_id, channel_id, from_node_id, to_node_id`); err != nil {
		return fmt.Errorf("storage: create link_aggregate view: %w", err)
	}
	return nil
}

func createGatewayDiversityView(db *sql.DB) error {
	if _, err := db.Exec(`DROP VIEW IF EXISTS gateway_diversity`); err != nil {
		return fmt.Errorf("storage: drop gateway_diversity view: %w", err)
	}
	if _, err := db.Exec(`CREATE VIEW gateway_diversity AS
	        SELECT
	            gateway_id,
	            COUNT(*) AS packets_total,
	            COUNT(DISTINCT from_node_id) AS unique_sources,
	            COUNT(DISTINCT to_node_id) AS unique_destinations,
	            AVG(hop_index) AS avg_hop_index,
	            AVG(rssi) AS avg_rssi,
	            AVG(snr) AS avg_snr,
	            MIN(received_at) AS first_seen,
	            MAX(received_at) AS last_seen
	        FROM link_history
	        GROUP BY gateway_id`); err != nil {
		return fmt.Errorf("storage: create gateway_diversity view: %w", err)
	}
	return nil
}

func createLongestLinksView(db *sql.DB) error {
	if _, err := db.Exec(`DROP VIEW IF EXISTS longest_links`); err != nil {
		return fmt.Errorf("storage: drop longest_links view: %w", err)
	}
	if _, err := db.Exec(`CREATE VIEW longest_links AS
	        SELECT
	            gateway_id,
	            from_node_id,
	            to_node_id,
	            COUNT(*) AS packets_total,
	            MAX(hop_index) AS max_hop_index,
	            MAX(hop_limit) AS max_hop_limit,
	            MIN(received_at) AS first_seen,
	            MAX(received_at) AS last_seen,
	            MAX(packet_id) AS last_packet_id
	        FROM link_history
	        GROUP BY gateway_id, from_node_id, to_node_id`); err != nil {
		return fmt.Errorf("storage: create longest_links view: %w", err)
	}
	return nil
}

func createTracerouteViews(db *sql.DB) error {
	if _, err := db.Exec(`DROP VIEW IF EXISTS traceroute_longest_paths`); err != nil {
		return fmt.Errorf("storage: drop traceroute_longest_paths: %w", err)
	}
	if _, err := db.Exec(`CREATE VIEW traceroute_longest_paths AS
	        SELECT
	            origin_node_id,
	            destination_node_id,
	            gateway_id,
	            MAX(hop_index + 1) AS max_hops,
	            COUNT(DISTINCT packet_id) AS observations,
	            MIN(received_at) AS first_seen,
	            MAX(received_at) AS last_seen
	        FROM traceroute_hops
	        WHERE direction = 'towards'
	        GROUP BY origin_node_id, destination_node_id, gateway_id`); err != nil {
		return fmt.Errorf("storage: create traceroute_longest_paths view: %w", err)
	}
	if _, err := db.Exec(`DROP VIEW IF EXISTS traceroute_hop_summary`); err != nil {
		return fmt.Errorf("storage: drop traceroute_hop_summary: %w", err)
	}
	if _, err := db.Exec(`CREATE VIEW traceroute_hop_summary AS
	        SELECT
	            gateway_id,
	            COUNT(*) AS hop_records,
	            COUNT(DISTINCT packet_id) AS packets_total,
	            AVG(hop_index + 1) AS avg_hops,
	            MAX(hop_index + 1) AS max_hops,
	            MIN(received_at) AS first_seen,
	            MAX(received_at) AS last_seen
	        FROM traceroute_hops
	        WHERE direction = 'towards'
	        GROUP BY gateway_id`); err != nil {
		return fmt.Errorf("storage: create traceroute_hop_summary view: %w", err)
	}
	return nil
}
func ensurePacketHistoryTimestamp(db *sql.DB) error {
	colType, err := columnType(db, "packet_history", "timestamp")
	if err != nil {
		return err
	}
	if colType == "" {
		return nil
	}
	if !strings.EqualFold(colType, "REAL") {
		if err := rebuildPacketHistoryTimestamp(db); err != nil {
			return err
		}
	}
	return normalizePacketHistoryTimestamp(db)
}

func rebuildPacketHistoryTimestamp(db *sql.DB) error {
	if _, err := db.Exec("PRAGMA foreign_keys=OFF"); err != nil {
		return fmt.Errorf("storage: disable foreign keys: %w", err)
	}
	defer db.Exec("PRAGMA foreign_keys=ON")

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("storage: begin rebuild packet_history: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.Exec(`ALTER TABLE packet_history RENAME TO packet_history_legacy`); err != nil {
		if strings.Contains(err.Error(), "no such table") {
			rollback = false
			_ = tx.Rollback()
			return nil
		}
		return fmt.Errorf("storage: rename packet_history: %w", err)
	}

	createStmt := `CREATE TABLE packet_history (
	        id INTEGER PRIMARY KEY AUTOINCREMENT,
	        timestamp REAL NOT NULL,
	        topic TEXT NOT NULL,
	        from_node_id INTEGER,
	        to_node_id INTEGER,
	        portnum INTEGER,
	        portnum_name TEXT,
	        gateway_id TEXT,
	        channel_id TEXT,
	        channel_name TEXT,
	        mesh_packet_id INTEGER,
	        rssi INTEGER,
	        snr REAL,
	        hop_limit INTEGER,
	        hop_start INTEGER,
	        payload_length INTEGER,
	        raw_payload BLOB,
	        processed_successfully INTEGER,
	        via_mqtt INTEGER,
	        want_ack INTEGER,
	        priority INTEGER,
	        delayed INTEGER,
	        channel_index INTEGER,
	        rx_time INTEGER,
	        pki_encrypted INTEGER,
	        next_hop INTEGER,
	        relay_node INTEGER,
	        tx_after INTEGER,
	        message_type TEXT,
	        raw_service_envelope BLOB,
	        parsing_error TEXT,
	        transport INTEGER,
	        qos INTEGER,
	        retained INTEGER
	    )`

	if _, err := tx.Exec(createStmt); err != nil {
		return fmt.Errorf("storage: recreate packet_history: %w", err)
	}

	columns, err := tableColumnNames(tx, "packet_history_legacy")
	if err != nil {
		return err
	}

	copySQL, err := buildPacketHistoryCopySQL(columns, "packet_history_legacy")
	if err != nil {
		return err
	}

	if _, err := tx.Exec(copySQL); err != nil {
		return fmt.Errorf("storage: copy packet_history rows: %w", err)
	}

	if _, err := tx.Exec(`DROP TABLE packet_history_legacy`); err != nil {
		return fmt.Errorf("storage: drop legacy packet_history: %w", err)
	}

	if err := resetPacketHistorySequence(tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("storage: commit packet_history rebuild: %w", err)
	}
	rollback = false
	return nil
}

func normalizePacketHistoryTimestamp(db *sql.DB) error {
	if _, err := db.Exec(`UPDATE packet_history SET timestamp = timestamp / 1000000.0 WHERE ABS(timestamp) >= 1000000000000`); err != nil {
		return fmt.Errorf("storage: normalize packet_history timestamp: %w", err)
	}
	return nil
}

type queryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

func tableColumnNames(q queryer, table string) ([]string, error) {
	rows, err := q.Query(fmt.Sprintf("PRAGMA table_info(%q)", table))
	if err != nil {
		return nil, fmt.Errorf("storage: table info %s: %w", table, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var (
			cid        int
			name       string
			typeName   string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &typeName, &notNull, &defaultVal, &pk); err != nil {
			return nil, fmt.Errorf("storage: scan table info %s: %w", table, err)
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: iterate table info %s: %w", table, err)
	}
	return columns, nil
}

func buildPacketHistoryCopySQL(columns []string, legacyTable string) (string, error) {
	if len(columns) == 0 {
		return "", errors.New("storage: legacy packet_history has no columns")
	}
	insertCols := make([]string, len(columns))
	selectCols := make([]string, len(columns))
	for i, col := range columns {
		insertCols[i] = col
		if strings.EqualFold(col, "timestamp") {
			selectCols[i] = "CASE WHEN ABS(timestamp) >= 1000000000000 THEN timestamp / 1000000.0 ELSE CAST(timestamp AS REAL) END AS timestamp"
		} else {
			selectCols[i] = col
		}
	}
	return fmt.Sprintf(`INSERT INTO packet_history (%s) SELECT %s FROM %s`, strings.Join(insertCols, ","), strings.Join(selectCols, ","), legacyTable), nil
}

func resetPacketHistorySequence(tx *sql.Tx) error {
	if _, err := tx.Exec(`DELETE FROM sqlite_sequence WHERE name='packet_history'`); err != nil && !isNoSuchTableErr(err) {
		return fmt.Errorf("storage: reset packet_history sequence (delete): %w", err)
	}
	if _, err := tx.Exec(`INSERT INTO sqlite_sequence(name, seq) SELECT 'packet_history', COALESCE(MAX(id), 0) FROM packet_history`); err != nil && !isNoSuchTableErr(err) {
		return fmt.Errorf("storage: reset packet_history sequence (insert): %w", err)
	}
	return nil
}

func columnType(db *sql.DB, table, column string) (string, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%q)", table))
	if err != nil {
		return "", fmt.Errorf("storage: table info %s: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name       string
			typeName   string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &typeName, &notNull, &defaultVal, &pk); err != nil {
			return "", fmt.Errorf("storage: scan column info %s.%s: %w", table, column, err)
		}
		if strings.EqualFold(name, column) {
			return typeName, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("storage: iterate column info %s.%s: %w", table, column, err)
	}
	return "", nil
}

func isNoSuchTableErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "no such table")
}

func columnExists(db *sql.DB, table, column string) (bool, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", table)
	rows, err := db.Query(query)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name       string
			typeName   string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &typeName, &notNull, &defaultVal, &pk); err != nil {
			return false, err
		}
		_ = cid
		_ = typeName
		_ = notNull
		_ = defaultVal
		_ = pk
		if strings.EqualFold(name, column) {
			return true, nil
		}
	}

	return false, rows.Err()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func uint32Ptr(v uint32) *uint32 {
	return &v
}

func snrPointer(values []int32, idx int) *float64 {
	if idx < 0 || idx >= len(values) {
		return nil
	}
	val := float64(values[idx]) / 4.0
	return &val
}

func statsSafe(stats *meshtasticpb.StoreAndForward_Statistics, getter func(*meshtasticpb.StoreAndForward_Statistics) uint32) *uint32 {
	if stats == nil {
		return nil
	}
	val := getter(stats)
	return &val
}

func historySafe(history *meshtasticpb.StoreAndForward_History, getter func(*meshtasticpb.StoreAndForward_History) uint32) *uint32 {
	if history == nil {
		return nil
	}
	val := getter(history)
	return &val
}

func heartbeatSafe(heartbeat *meshtasticpb.StoreAndForward_Heartbeat, getter func(*meshtasticpb.StoreAndForward_Heartbeat) uint32) *uint32 {
	if heartbeat == nil {
		return nil
	}
	val := getter(heartbeat)
	return &val
}

func nullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullBytes(b []byte) interface{} {
	if len(b) == 0 {
		return nil
	}
	return b
}

func nullFloat64(v *float64) interface{} {
	if v == nil {
		return nil
	}
	return *v
}

func nullInt32(v *int32) interface{} {
	if v == nil {
		return nil
	}
	return int64(*v)
}

func nullUint32(u *uint32) interface{} {
	if u == nil {
		return nil
	}
	return int64(*u)
}

func (w *SQLiteWriter) publishErr(err error) {
	if err == nil {
		return
	}
	w.logger.Error("storage error", slog.Any("error", err))
}
