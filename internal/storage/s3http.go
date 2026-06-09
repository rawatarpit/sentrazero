package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const sha256EmptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

type S3HTTPBackend struct {
	bucketName string
	endpoint string // full endpoint like https://project.storage.supabase.co/storage/v1/s3
	host      string // just the host
	accessKey string
	secretKey string
	client   *http.Client
	region  string
}

func NewS3HTTPBackend(endpoint, bucketName, region string, creds *S3Credentials) (*S3HTTPBackend, error) {
	log.Printf("[storage] S3HTTP: endpoint=%s bucket=%s region=%s", endpoint, bucketName, region)

	// Parse just the host from endpoint
	endpoint = strings.TrimSuffix(endpoint, "/")
	u, _ := url.Parse(endpoint)
	host := u.Host

	// Custom transport with TLS settings
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	
	// Pre-connect to warm up TLS
	conn, err := tls.Dial("tcp", host+":443", tr.TLSClientConfig)
	if err != nil {
		log.Printf("[storage] TLS pre-connect failed: %v (continuing anyway)", err)
	} else {
		conn.Close()
		log.Printf("[storage] TLS pre-connect OK, version: %d", conn.ConnectionState().Version)
	}

	log.Printf("[storage] S3HTTP: host=%s bucket=%s region=%s", host, bucketName, region)

	return &S3HTTPBackend{
		bucketName: bucketName,
		endpoint:   endpoint,
		host:     host,
		accessKey: strings.TrimSpace(creds.AccessKeyID),
		secretKey: strings.TrimSpace(creds.SecretAccessKey),
		region:   strings.TrimSpace(region),
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: tr,
		},
	}, nil
}

func (b *S3HTTPBackend) signRequest(req *http.Request, payloadHash string) error {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	// Parse URL to get path and query
	parsedURL := req.URL
	canonicalURI := parsedURL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	
	// Encode query parameters - must be sorted alphabetically
	queries := parsedURL.Query()
	sortedKeys := make([]string, 0, len(queries))
	for k := range queries {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	
	var canonicalQuery string
	for i, k := range sortedKeys {
		for _, v := range queries[k] {
			if i > 0 {
				canonicalQuery += "&"
			}
			canonicalQuery += url.QueryEscape(k) + "=" + url.QueryEscape(v)
		}
	}

	// Host header
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("Host", b.host)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	// Canonical headers - MUST be lowercase and sorted alphabetically
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		b.host, payloadHash, amzDate)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	// Canonical request - order matters!
	canonicalParts := []string{
		req.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}
	canonicalRequest := strings.Join(canonicalParts, "\n")
	
	log.Printf("[storage] sign - Method: %s", req.Method)
	log.Printf("[storage] sign - URI: %s", canonicalURI)
	log.Printf("[storage] sign - Query: %s", canonicalQuery)
	log.Printf("[storage] sign - Headers: %s", canonicalHeaders)
	log.Printf("[storage] sign - canonicalRequest:\n%s", canonicalRequest)

	// Hash canonical request
	hash := sha256.Sum256([]byte(canonicalRequest))
	canonicalRequestHash := hex.EncodeToString(hash[:])

	// Credential scope
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, b.region)

	// String to sign
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate,
		credentialScope,
		canonicalRequestHash,
	)
	
	log.Printf("[storage] sign - CredentialScope: %s", credentialScope)
	log.Printf("[storage] sign - StringToSign:\n%s", stringToSign)

	// Derive signing key - same as AWS
	kDate := hmacSHA256([]byte("AWS4"+b.secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, b.region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")

	// Calculate signature
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	// Authorization header
	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		b.accessKey, credentialScope, signedHeaders, signature)
	
	req.Header.Set("Authorization", authHeader)

	log.Printf("[storage] sign - Signature: %s", signature[:20]+"...")
	log.Printf("[storage] sign - Auth: %s", authHeader[:50]+"...")

	return nil
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func (b *S3HTTPBackend) buildURL(objectKey string) string {
	// PATH-STYLE: https://host/bucket/key (NOT virtual-hosted)
	if objectKey == "" || objectKey == "/" {
		return fmt.Sprintf("%s/%s", b.endpoint, b.bucketName)
	}
	// If key starts with ?, it's a query - build properly
	if strings.HasPrefix(objectKey, "?") {
		return fmt.Sprintf("%s/%s%s", b.endpoint, b.bucketName, objectKey)
	}
	// Normal path - trim leading slash
	if strings.HasPrefix(objectKey, "/") {
		objectKey = objectKey[1:]
	}
	return fmt.Sprintf("%s/%s/%s", b.endpoint, b.bucketName, objectKey)
}

func (b *S3HTTPBackend) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	log.Printf("[storage] S3HTTP ListObjects: bucket=%s prefix=%s", b.bucketName, prefix)
	
	// PATH-style URL: https://host/storage/v1/s3/bucket?query
	queryParams := "list-type=2&prefix=" + url.QueryEscape(prefix) + "&max-keys=1000"
	urlStr := fmt.Sprintf("%s/%s?%s", b.endpoint, b.bucketName, queryParams)
	
	log.Printf("[storage] ListObjects URL (path-style): %s", urlStr)
	
	// Retry logic for TLS errors
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			log.Printf("[storage] ListObjects retry %d after error: %v", attempt, lastErr)
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
	
		req, err := http.NewRequest("GET", urlStr, nil)
		if err != nil {
			return nil, err
		}
		
		if err := b.signRequest(req, sha256EmptyHash); err != nil {
			return nil, err
		}

		resp, err := b.client.Do(req)
		if err != nil {
			errStr := err.Error()
			// Check if TLS/network error - retry
			if strings.Contains(errStr, "tls:") || strings.Contains(errStr, "network") {
				lastErr = err
				continue
			}
			return nil, err
		}
		defer resp.Body.Close()

		// Handle response
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		
		if resp.StatusCode >= 400 {
			errStr := string(body)
			// If TLS error in response - retry
			if strings.Contains(errStr, "tls:") || strings.Contains(errStr, "handshake") {
				lastErr = fmt.Errorf("TLS error: %s", errStr)
				continue
			}
			return nil, fmt.Errorf("S3 HTTP %d: %s", resp.StatusCode, errStr)
		}

		// Success
		objects := parseListObjectsV2(string(body))
		log.Printf("[storage] ListObjects: found %d objects", len(objects))
		return objects, nil
	}
	
	return nil, fmt.Errorf("ListObjects failed after 3 retries: %w", lastErr)
}

func parseListObjectsV2(xmlStr string) []ObjectInfo {
	var objects []ObjectInfo
	
	// Simple XML parsing for <Key> and <Size>
	for {
		start := strings.Index(xmlStr, "<Key>")
		if start < 0 {
			break
		}
		end := strings.Index(xmlStr[start:], "</Key>")
		if end < 0 {
			break
		}
		
		keyStart := start + len("<Key>")
		key := xmlStr[keyStart:start+end]
		xmlStr = xmlStr[start+end+len("</Key>"):]
		
		// Get size  
		sizeStart := strings.Index(xmlStr, "<Size>")
		sizeEnd := strings.Index(xmlStr[sizeStart:], "</Size>")
		var size int64 = 0
		if sizeStart >= 0 && sizeEnd > 0 {
			fmt.Sscanf(xmlStr[sizeStart+len("<Size>"):sizeStart+sizeEnd], "%d", &size)
		}
		
		if key == "" {
			continue
		}
		
		objects = append(objects, ObjectInfo{Key: key, Size: size})
	}

	if objects == nil {
		objects = []ObjectInfo{}
	}
	
	return objects
}

func (b *S3HTTPBackend) ReadObject(ctx context.Context, remotePath string) (io.ReadCloser, error) {
	log.Printf("[storage] S3HTTP ReadObject: %s", remotePath)
	
	urlStr := b.buildURL(remotePath)
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	
	if err := b.signRequest(req, sha256EmptyHash); err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return resp.Body, nil
}

func (b *S3HTTPBackend) WriteObject(ctx context.Context, remotePath string, reader io.Reader) error {
	log.Printf("[storage] S3HTTP WriteObject: %s", remotePath)

	body, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read payload: %w", err)
	}

	payloadHash := sha256Hex(body)

	urlStr := b.buildURL(remotePath)
	req, err := http.NewRequest("PUT", urlStr, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(body))
	req.Header.Set("Content-Type", "application/octet-stream")

	if err := b.signRequest(req, payloadHash); err != nil {
		return err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func (b *S3HTTPBackend) StatObject(ctx context.Context, remotePath string) (ObjectInfo, error) {
	urlStr := b.buildURL(remotePath)
	req, err := http.NewRequest("HEAD", urlStr, nil)
	if err != nil {
		return ObjectInfo{}, err
	}
	
	if err := b.signRequest(req, sha256EmptyHash); err != nil {
		return ObjectInfo{}, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return ObjectInfo{}, fmt.Errorf("not found")
	}
	if resp.StatusCode >= 400 {
		return ObjectInfo{}, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	return ObjectInfo{Key: remotePath, Size: resp.ContentLength}, nil
}

func (b *S3HTTPBackend) DeleteObject(ctx context.Context, remotePath string) error {
	urlStr := b.buildURL(remotePath)
	req, err := http.NewRequest("DELETE", urlStr, nil)
	if err != nil {
		return err
	}
	
	if err := b.signRequest(req, sha256EmptyHash); err != nil {
		return err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil
}