package httpclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"sentra-agent/internal/obs"
)

const (
	DefaultTimeout    = 10 * time.Second
	DefaultMaxRetries = 3
	InitialBackoff    = 200 * time.Millisecond
	MaxBackoff        = 1 * time.Second
)

type Client struct {
	baseURL    string
	anonKey    string
	agentToken string
	http       *http.Client
	maxRetries int
}

type Option func(*Client)

func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.http.Timeout = timeout
	}
}

func WithMaxRetries(maxRetries int) Option {
	return func(c *Client) {
		c.maxRetries = maxRetries
	}
}

func NewClient(baseURL, anonKey, agentToken string, opts ...Option) *Client {
	if agentToken == "" {
		log.Panic("[HTTPCLIENT] FATAL: agentToken cannot be empty - this indicates a configuration bug")
	}

	c := &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		anonKey:    anonKey,
		agentToken: agentToken,
		http: &http.Client{
			Timeout: DefaultTimeout,
		},
		maxRetries: DefaultMaxRetries,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *Client) BaseURL() string {
	return c.baseURL
}

func (c *Client) NewRequest(method, path string, body []byte) (*http.Request, error) {
	url := c.baseURL + path

	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+c.anonKey)
	req.Header.Set("x-agent-token", c.agentToken)
	req.Header.Set("Content-Type", "application/json")

	log.Printf("[HTTPCLIENT] DEBUG x-agent-token: %s", MaskToken(c.agentToken))

	return req, nil
}

func (c *Client) Do(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	return c.DoWithReq(ctx, method, path, body, nil)
}

func (c *Client) DoWithReq(ctx context.Context, method, path string, body []byte, extraHeaders func(*http.Request)) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		req, err := c.NewRequest(method, path, body)
		if err != nil {
			return nil, err
		}

		req = req.WithContext(ctx)

		if extraHeaders != nil {
			extraHeaders(req)
		}

		hasAuth := false
		hasAgentToken := false
		for k, v := range req.Header {
			if strings.EqualFold(k, "authorization") {
				hasAuth = true
			}
			if strings.EqualFold(k, "x-agent-token") {
				hasAgentToken = true
			}
			if strings.EqualFold(k, "authorization") || strings.EqualFold(k, "x-agent-token") {
				obs.Debug("http_header", obs.Field{"header": k, "value": MaskToken(v[0])})
			}
		}
		if !hasAuth || !hasAgentToken {
			obs.Error("http_header_missing", obs.Field{
				"has_auth": hasAuth, "has_agent_token": hasAgentToken,
				"method": method, "path": path,
				"agent_token_len": len(c.agentToken),
			})
			return nil, fmt.Errorf("required headers missing: Authorization=%v, x-agent-token=%v", hasAuth, hasAgentToken)
		}

		start := time.Now()

		resp, err := c.http.Do(req)
		latency := time.Since(start)

		if err != nil {
			obs.Warn("http request failed",
				obs.Field{"method": method, "path": path, "attempt": attempt + 1, "latency_ms": latency.Milliseconds(), "error": err.Error()},
			)

			if attempt < c.maxRetries {
				backoff := InitialBackoff * time.Duration(1<<uint(attempt))
				if backoff > MaxBackoff {
					backoff = MaxBackoff
				}
				time.Sleep(backoff)
				continue
			}
			return nil, err
		}

		logResp := obs.Field{
			"method":     method,
			"path":       path,
			"status":     resp.StatusCode,
			"latency_ms": latency.Milliseconds(),
			"attempt":    attempt + 1,
		}

		if resp.StatusCode >= 500 {
			obs.Warn("http server error", logResp)

			resp.Body.Close()

			if attempt < c.maxRetries {
				backoff := InitialBackoff * time.Duration(1<<uint(attempt))
				if backoff > MaxBackoff {
					backoff = MaxBackoff
				}
				time.Sleep(backoff)
				continue
			}
			return resp, nil
		}

		if resp.StatusCode < 300 {
			obs.Info("http request success", logResp)
		}

		return resp, nil
	}

	return nil, lastErr
}

func (c *Client) Post(ctx context.Context, path string, body []byte) (*http.Response, error) {
	return c.Do(ctx, http.MethodPost, path, body)
}

func (c *Client) Get(ctx context.Context, path string) (*http.Response, error) {
	return c.Do(ctx, http.MethodGet, path, nil)
}

func (c *Client) PostWithHeaders(ctx context.Context, path string, body []byte, extraHeaders func(*http.Request)) (*http.Response, error) {
	return c.DoWithReq(ctx, http.MethodPost, path, body, extraHeaders)
}

func MaskToken(token string) string {
	if len(token) <= 8 {
		return "***"
	}
	return token[:4] + "***" + token[len(token)-4:]
}

func ReadBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
