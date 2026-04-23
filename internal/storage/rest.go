package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

type RESTBackend struct {
	bucketName string
	baseURL    string
	anonKey    string
	token     string
	client    *http.Client
}

func NewRESTBackend(bucketName, endpoint string) (*RESTBackend, error) {
	// Extract base URL from Supabase endpoint
	// endpoint: https://project.storage.supabase.co/storage/v1/s3
	// baseURL: https://project.supabase.co
	baseURL := endpoint
	if strings.Contains(endpoint, ".storage.supabase.co") {
		baseURL = strings.Replace(endpoint, ".storage.supabase.co", ".supabase.co", 1)
	}
	baseURL = strings.TrimSuffix(baseURL, "/storage/v1/s3")

	// Get credentials from environment
	anonKey := GetGlobalAnonKey()
	token := GetGlobalToken()

	if anonKey == "" {
		// Fall back to empty - will use public URLs
		anonKey = ""
	}

	return &RESTBackend{
		bucketName: bucketName,
		baseURL:    baseURL,
		anonKey:    anonKey,
		token:      token,
		client:    &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func GetGlobalAnonKey() string {
	return globalAnonKey
}

func GetGlobalToken() string {
	return globalToken
}

func (b *RESTBackend) makeRequest(method, path string, body io.Reader) ([]byte, error) {
	url := b.baseURL + "/storage/v1/" + path
	
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	headers := map[string]string{
		"Authorization": "Bearer " + b.token,
		"apikey":        b.anonKey,
	}
	if b.anonKey != "" {
		req.Header.Set("Authorization", "Bearer "+b.anonKey)
		req.Header.Set("apikey", b.anonKey)
	}
	if b.token != "" {
		req.Header.Set("Authorization", "Bearer "+b.token)
	}
	
	for k, v := range headers {
		if v != "" {
			req.Header.Set(k, v)
		}
	}

	if method == "POST" || method == "PUT" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

func (b *RESTBackend) ReadObject(ctx context.Context, remotePath string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/storage/v1/object/%s/%s", b.baseURL, b.bucketName, remotePath)
	
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	
	if b.anonKey != "" {
		req.Header.Set("Authorization", "Bearer "+b.anonKey)
		req.Header.Set("apikey", b.anonKey)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	return resp.Body, nil
}

func (b *RESTBackend) WriteObject(ctx context.Context, remotePath string, reader io.Reader) error {
	// For writing, we'd need to implement multipart upload
	// For now, fall back to error
	return fmt.Errorf("WriteObject not implemented for REST backend - use S3 or shared_mount")
}

type RESTListResponse struct {
	Keys []struct {
		Name            string `json:"name"`
		Id              string `json:"id"`
		UpdatedAt       string `json:"updated_at"`
		CreatedAt       string `json:"created_at"`
		LastAccessedAt  string `json:"last_accessed_at"`
		Metadata        any    `json:"metadata"`
		 Buckets        any    `json:"buckets"`
	} `json:"keys"`
}

func (b *RESTBackend) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	// Use list API
	path := fmt.Sprintf("object/list/%s", b.bucketName)
	if prefix != "" {
		path += "?prefix=" + prefix
	}

	respBody, err := b.makeRequest("POST", path, strings.NewReader(`{"prefix":"","recursive":true,"limit":1000}`))
	if err != nil {
		return nil, err
	}

	var listResp RESTListResponse
	if err := json.Unmarshal(respBody, &listResp); err != nil {
		return nil, fmt.Errorf("failed to parse list response: %w", err)
	}

	results := make([]ObjectInfo, 0, len(listResp.Keys))
	for _, key := range listResp.Keys {
		obj := ObjectInfo{
			Key: key.Name,
		}
		// Parse timestamps
		if key.UpdatedAt != "" {
			if t, err := time.Parse(time.RFC3339, key.UpdatedAt); err == nil {
				obj.LastModified = t
			}
		}
		results = append(results, obj)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	return results, nil
}

func (b *RESTBackend) StatObject(ctx context.Context, remotePath string) (ObjectInfo, error) {
	path := fmt.Sprintf("object/info/%s/%s", b.bucketName, remotePath)
	
	respBody, err := b.makeRequest("GET", path, nil)
	if err != nil {
		return ObjectInfo{}, err
	}

	var info struct {
		Name            string `json:"name"`
		UpdatedAt       string `json:"updated_at"`
		Metadata        any    `json:"metadata"`
		ContentLength   int64  `json:"content_length"`
	}
	if err := json.Unmarshal(respBody, &info); err != nil {
		return ObjectInfo{}, err
	}

	obj := ObjectInfo{
		Key: info.Name,
		Size: info.ContentLength,
	}
	if info.UpdatedAt != "" {
		if t, err := time.Parse(time.RFC3339, info.UpdatedAt); err == nil {
			obj.LastModified = t
		}
	}

	return obj, nil
}

func (b *RESTBackend) DeleteObject(ctx context.Context, remotePath string) error {
	path := fmt.Sprintf("object/%s/%s", b.bucketName, remotePath)
	
	_, err := b.makeRequest("DELETE", path, nil)
	return err
}