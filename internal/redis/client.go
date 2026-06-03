package redis

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	client    *redis.Client
	pubsub    *redis.PubSub
	mu        sync.RWMutex
	connected bool
}

type Config struct {
	URL      string
	Password string
	DB       int
	PoolSize int
	UseTLS   bool
}

type JobQueue struct {
	client    *Client
	streamKey string
	group     string
	consumer  string
}

func NewClient(cfg *Config) (*Client, error) {
	opts := &redis.Options{
		Addr:     cfg.URL,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	}
	if cfg.UseTLS {
		opts.TLSConfig = &tls.Config{}
	}
	rdb := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &Client{
		client:    rdb,
		connected: true,
	}, nil
}

// NewClientFromURL parses a Redis URL (redis:// or rediss://) and creates a client.
// This handles the standard format: rediss://default:<password>@<host>:<port>/<db>
func NewClientFromURL(redisURL string) (*Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis URL: %w", err)
	}
	rdb := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &Client{
		client:    rdb,
		connected: true,
	}, nil
}

func (c *Client) Close() error {
	if c.pubsub != nil {
		c.pubsub.Close()
	}
	return c.client.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *Client) SetConnected(connected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = connected
}

func (c *Client) EnqueueJob(ctx context.Context, jobID string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	return c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: "sentra:jobs",
		Values: map[string]interface{}{
			"job_id":    jobID,
			"data":      string(data),
			"queued_at": time.Now().Unix(),
		},
	}).Err()
}

func (c *Client) DequeueJob(ctx context.Context, consumer string, block time.Duration) (string, map[string]interface{}, error) {
	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "sentra:workers",
		Consumer: consumer,
		Streams:  []string{"sentra:jobs", "0"},
		Count:    1,
		Block:    block,
	}).Result()

	if err == redis.Nil {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}

	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return "", nil, nil
	}

	msg := streams[0].Messages[0]
	jobID := msg.Values["job_id"]
	payload := msg.Values["data"]

	return msg.ID, map[string]interface{}{
		"job_id": jobID,
		"data":   payload,
	}, nil
}

func (c *Client) AckJob(ctx context.Context, streamKey, messageID string) error {
	return c.client.XAck(ctx, streamKey, "sentra:workers", messageID).Err()
}

func (c *Client) RequeueJob(ctx context.Context, streamKey, messageID string, delay time.Duration) error {
	if delay > 0 {
		return c.client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]interface{}{
				"message_id": messageID,
				"retry_at":   time.Now().Add(delay).Unix(),
			},
		}).Err()
	}
	return c.client.XAck(ctx, streamKey, "sentra:workers", messageID).Err()
}

func (c *Client) Publish(ctx context.Context, channel string, message interface{}) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return c.client.Publish(ctx, channel, data).Err()
}

func (c *Client) Subscribe(ctx context.Context, channel string) *redis.PubSub {
	return c.client.Subscribe(ctx, channel)
}

func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, key, data, expiration).Err()
}

func (c *Client) Get(ctx context.Context, key string, dest interface{}) error {
	data, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dest)
}

func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	return c.client.Incr(ctx, key).Result()
}

func (c *Client) GetAllByPattern(ctx context.Context, pattern string) (map[string]interface{}, error) {
	keys, err := c.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	for _, key := range keys {
		val, err := c.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		result[key] = val
	}
	return result, nil
}

func (c *Client) CreateConsumerGroup(ctx context.Context, stream, group string) error {
	err := c.client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (c *Client) UpdateWorkerStatus(ctx context.Context, workerID string, status map[string]interface{}) error {
	data, err := json.Marshal(status)
	if err != nil {
		return err
	}
	return c.client.HSet(ctx, "sentra:workers:"+workerID, map[string]interface{}{
		"status":  string(data),
		"updated": time.Now().Unix(),
	}).Err()
}

func (c *Client) GetActiveWorkers(ctx context.Context) (map[string]interface{}, error) {
	keys, err := c.client.Keys(ctx, "sentra:workers:*").Result()
	if err != nil {
		return nil, err
	}

	workers := make(map[string]interface{})
	for _, key := range keys {
		workerID := key[len("sentra:workers:"):]
		data, err := c.client.HGetAll(ctx, key).Result()
		if err != nil {
			continue
		}
		workers[workerID] = data
	}
	return workers, nil
}

func (c *Client) Del(ctx context.Context, keys ...string) error {
	return c.client.Del(ctx, keys...).Err()
}

func (c *Client) MarkJobProcessed(ctx context.Context, jobID string, ttl time.Duration) (bool, error) {
	return c.client.SetNX(ctx, "sentra:dedup:"+jobID, "1", ttl).Result()
}

func (c *Client) IsJobProcessed(ctx context.Context, jobID string) (bool, error) {
	exists, err := c.client.Exists(ctx, "sentra:dedup:"+jobID).Result()
	return exists == 1, err
}
