package storage

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
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

	return &S3HTTPBackend{
		bucketName: bucketName,
		endpoint:   endpoint,
		host:     host,
		accessKey: creds.AccessKeyID,
		secretKey: creds.SecretAccessKey,
		region:   region,
		client:   &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (b *S3HTTPBackend) signRequest(req *http.Request) error {
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
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	// Canonical headers - MUST be lowercase and sorted alphabetically
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		b.host, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", amzDate)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	// Unsigned payload
	payloadHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

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
	// Virtual-hosted style: https://bucket.host/path/key
	// This is what AWS CLI uses!
	if objectKey == "" || objectKey == "/" {
		return fmt.Sprintf("https://%s.%s/storage/v1/s3/%s", b.bucketName, b.host, b.bucketName)
	}
	// Handle query string in object key
	if strings.Contains(objectKey, "?") {
		parts := strings.SplitN(objectKey, "?", 2)
		key := parts[0]
		query := parts[1]
		if key == "" {
			return fmt.Sprintf("https://%s.%s/storage/v1/s3?%s", b.bucketName, b.host, query)
		}
		return fmt.Sprintf("https://%s.%s/storage/v1/s3/%s?%s", b.bucketName, b.host, key, query)
	}
	// Regular key
	if strings.HasPrefix(objectKey, "/") {
		objectKey = objectKey[1:]
	}
	return fmt.Sprintf("https://%s.%s/storage/v1/s3/%s", b.bucketName, b.host, objectKey)
}

func (b *S3HTTPBackend) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	log.Printf("[storage] S3HTTP ListObjects: bucket=%s prefix=%s", b.bucketName, prefix)
	
	// Build URL with query params - virtual-hosted style
	// Format: https://bucket.host/storage/v1/s3?list-type=2&prefix=xxx
	queryParams := "list-type=2&prefix=" + url.QueryEscape(prefix) + "&max-keys=1000"
	urlStr := fmt.Sprintf("https://%s.%s/storage/v1/s3?%s", b.bucketName, b.host, queryParams)
	
	log.Printf("[storage] ListObjects URL: %s", urlStr)
	
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	
	if err := b.signRequest(req); err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		log.Printf("[storage] ListObjects request failed: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	bodyStr := string(body)
	if len(bodyStr) > 300 {
		bodyStr = bodyStr[:300] + "..."
	}
	log.Printf("[storage] Response status: %d body: %s", resp.StatusCode, bodyStr)

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("S3 HTTP %d: %s", resp.StatusCode, string(body))
	}

	// Parse XML manually  
	objects := parseListObjectsV2(string(body))
	log.Printf("[storage] Found %d objects", len(objects))
	return objects, nil
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
	
	if err := b.signRequest(req); err != nil {
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
	return fmt.Errorf("WriteObject not implemented")
}

func (b *S3HTTPBackend) StatObject(ctx context.Context, remotePath string) (ObjectInfo, error) {
	urlStr := b.buildURL(remotePath)
	req, err := http.NewRequest("HEAD", urlStr, nil)
	if err != nil {
		return ObjectInfo{}, err
	}
	
	if err := b.signRequest(req); err != nil {
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
	
	if err := b.signRequest(req); err != nil {
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