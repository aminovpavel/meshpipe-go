package mqtt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
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
	client   mqtt.Client
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
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", c.cfg.BrokerHost, c.cfg.BrokerPort))
	opts.SetOrderMatters(false)
	opts.SetKeepAlive(c.cfg.KeepAlive)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(c.cfg.ReconnectGap)
	opts.SetAutoReconnect(true)

	if c.cfg.ClientID != "" {
		opts.SetClientID(c.cfg.ClientID)
	}
	if c.cfg.Username != "" {
		opts.SetUsername(c.cfg.Username)
		opts.SetPassword(c.cfg.Password)
	}

	topic := c.cfg.SubscriptionTopic()

	opts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
		select {
		case c.messages <- Message{
			Topic:    msg.Topic(),
			Payload:  append([]byte(nil), msg.Payload()...),
			QoS:      msg.Qos(),
			Retained: msg.Retained(),
			Time:     time.Now(),
		}:
		default:
			log.Printf("mqtt: dropping message, channel full (topic=%s)", msg.Topic())
		}
	})

	opts.OnConnect = func(m mqtt.Client) {
		token := m.Subscribe(topic, 0, nil)
		token.Wait()
		if err := token.Error(); err != nil {
			c.publishErr(fmt.Errorf("mqtt: subscribe failed for %s: %w", topic, err))
		} else {
			log.Printf("mqtt: subscribed to %s", topic)
		}
	}

	opts.OnConnectionLost = func(_ mqtt.Client, err error) {
		c.publishErr(fmt.Errorf("mqtt: connection lost: %w", err))
	}

	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		return fmt.Errorf("mqtt: connect failed: %w", err)
	}

	c.client = client

	go func() {
		<-ctx.Done()
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
		if c.client != nil && c.client.IsConnected() {
			c.client.Disconnect(250)
		}
		close(c.messages)
		close(c.errs)
	})
}

func (c *Client) publishErr(err error) {
	if err == nil {
		return
	}
	select {
	case c.errs <- err:
	default:
		log.Printf("mqtt: dropping error: %v", err)
	}
}
