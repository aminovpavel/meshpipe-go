package mqtt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

const (
	defaultKeepAlive          = 30 * time.Second
	defaultConnectRetry       = 5 * time.Second
	defaultMessageBufferDepth = 1024
)

// Config holds connection parameters for the MQTT broker.
type Config struct {
	BrokerHost   string
	BrokerPort   int
	Username     string
	Password     string
	TopicPrefix  string
	TopicSuffix  string
	ClientID     string
	KeepAlive    time.Duration
	ReconnectGap time.Duration
}

// SubscriptionTopic joins prefix and suffix into a valid MQTT subscription topic.
func (c Config) SubscriptionTopic() string {
	prefix := strings.TrimSuffix(c.TopicPrefix, "/")
	suffix := strings.TrimPrefix(c.TopicSuffix, "/")

	switch {
	case prefix == "" && suffix == "":
		return "#"
	case prefix == "":
		return suffix
	case suffix == "":
		return prefix
	default:
		return prefix + "/" + suffix
	}
}

func (c *Config) normalise() {
	if c.KeepAlive == 0 {
		c.KeepAlive = defaultKeepAlive
	}
	if c.ReconnectGap == 0 {
		c.ReconnectGap = defaultConnectRetry
	}
}

func (c Config) validate() error {
	if strings.TrimSpace(c.BrokerHost) == "" {
		return errors.New("mqtt: broker host must be provided")
	}
	if c.BrokerPort <= 0 {
		return errors.New("mqtt: broker port must be positive")
	}
	return nil
}

// Message represents a received MQTT message.
type Message struct {
	Topic    string
	Payload  []byte
	QoS      byte
	Retained bool
	Time     time.Time
}

// Client manages MQTT connectivity and exposes an async message stream.
type Client struct {
	cfg      Config
	manager  *autopaho.ConnectionManager
	cancel   context.CancelFunc
	messages chan Message
	errs     chan error
	stopOnce sync.Once
}

// NewClient creates a Client with the given configuration.
func NewClient(cfg Config) (*Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.normalise()

	return &Client{
		cfg:      cfg,
		messages: make(chan Message, defaultMessageBufferDepth),
		errs:     make(chan error, 16),
	}, nil
}

// Messages returns a read-only channel with incoming MQTT messages.
func (c *Client) Messages() <-chan Message {
	return c.messages
}

// Errors returns asynchronous error notifications (connection loss, subscribe failures, etc.).
func (c *Client) Errors() <-chan error {
	return c.errs
}

// Start connects to the broker and begins streaming messages until the context is cancelled.
func (c *Client) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	topic := c.cfg.SubscriptionTopic()

	brokerURL := &url.URL{
		Scheme: "mqtt",
		Host:   fmt.Sprintf("%s:%d", c.cfg.BrokerHost, c.cfg.BrokerPort),
	}

	clientCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{brokerURL},
		CleanStartOnInitialConnection: true,
		OnConnectionUp: func(m *autopaho.ConnectionManager, _ *paho.Connack) {
			go func() {
				subCtx, cancelSub := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancelSub()
				_, err := m.Subscribe(subCtx, &paho.Subscribe{
					Subscriptions: []paho.SubscribeOptions{
						{Topic: topic, QoS: byte(0)},
					},
				})
				if err != nil {
					c.publishErr(fmt.Errorf("mqtt: subscribe failed for %s: %w", topic, err))
				} else {
					log.Printf("mqtt: subscribed to %s", topic)
				}
			}()
		},
		OnConnectError: func(err error) {
			c.publishErr(fmt.Errorf("mqtt: connection error: %w", err))
		},
		ClientConfig: paho.ClientConfig{
			ClientID: c.cfg.ClientID,
			OnClientError: func(err error) {
				c.publishErr(fmt.Errorf("mqtt: client error: %w", err))
			},
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					pub := pr.Packet
					c.publishMessage(Message{
						Topic:    pub.Topic,
						Payload:  append([]byte(nil), pub.Payload...),
						QoS:      pub.QoS,
						Retained: pub.Retain,
						Time:     time.Now(),
					})
					return true, nil
				},
			},
		},
	}

	if c.cfg.KeepAlive > 0 {
		ka := uint16(c.cfg.KeepAlive / time.Second)
		if ka == 0 {
			ka = 1
		}
		clientCfg.KeepAlive = ka
	}

	if c.cfg.ReconnectGap > 0 {
		delay := c.cfg.ReconnectGap
		clientCfg.ReconnectBackoff = func(int) time.Duration { return delay }
	}

	if c.cfg.Username != "" {
		clientCfg.ConnectUsername = c.cfg.Username
		if c.cfg.Password != "" {
			clientCfg.ConnectPassword = []byte(c.cfg.Password)
		}
	}

	manager, err := autopaho.NewConnection(runCtx, clientCfg)
	if err != nil {
		cancel()
		return fmt.Errorf("mqtt: initialise connection manager: %w", err)
	}

	if err := manager.AwaitConnection(runCtx); err != nil && !errors.Is(err, context.Canceled) {
		cancel()
		return fmt.Errorf("mqtt: await connection: %w", err)
	}

	c.manager = manager
	c.cancel = cancel

	go func() {
		<-runCtx.Done()
		c.stop()
	}()

	return nil
}

// Stop terminates the MQTT session and closes channels.
func (c *Client) Stop() {
	c.stop()
}

func (c *Client) stop() {
	c.stopOnce.Do(func() {
		if c.manager != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.manager.Disconnect(ctx)
			cancel()
		}
		if c.cancel != nil {
			c.cancel()
		}
		close(c.messages)
		close(c.errs)
	})
}

func (c *Client) publishErr(err error) {
	if err == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Printf("mqtt: dropping error (channel closed): %v", err)
		}
	}()
	select {
	case c.errs <- err:
	default:
		log.Printf("mqtt: dropping error: %v", err)
	}
}

func (c *Client) publishMessage(msg Message) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("mqtt: dropping message (channel closed) topic=%s", msg.Topic)
		}
	}()
	select {
	case c.messages <- msg:
	default:
		log.Printf("mqtt: dropping message, channel full (topic=%s)", msg.Topic)
	}
}
