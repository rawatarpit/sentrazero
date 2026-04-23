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
	"strings"
	"time"
)

type S3HTTPBackend struct {
	bucketName string
	endpoint string
	accessKey string
	secretKey string
	client  *http.Client
	region  string
}

func NewS3HTTPBackend(endpoint, bucketName, region string, creds *S3Credentials) (*S3HTTPBackend, error) {
	log.Printf("[storage] S3HTTP: endpoint=%s bucket=%s region=%s", endpoint, bucketName, region)

	// endpoint format: https://project.storage.supabase.co/storage/v1/s3
	// we need host only and use path-style for requests
	return &S3HTTPBackend{
		bucketName: bucketName,
		endpoint: strings.TrimSuffix(endpoint, "/"),
		accessKey: creds.AccessKeyID,
		secretKey: creds.SecretAccessKey,
		region:   region,
		client:   &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (b *S3HTTPBackend) signRequest(req *http.Request) error {
	// AWS Signature Version 4 - simplified for Supabase Storage S3
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	// Host header
	host := req.URL.Host
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("Host", host)

	// Canonical request
	canonicalURI := req.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	canonicalQuery := ""
	if req.URL.RawQuery != "" {
		canonicalQuery = req.URL.RawQuery
	}

	// Signed headers - must be lowercase and sorted
	signedHeaders := "host;x-amz-date"
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-date:%s\n", host, amzDate)

	// Unsigned payload
	payloadHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	
	// Canonical request string
	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		req.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)
	
	log.Printf("[storage] canonicalRequest: %s", canonicalRequest)

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
	
	log.Printf("[storage] stringToSign: %s", stringToSign)

	// Derive signing key
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
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	log.Printf("[storage] Authorization: %s", authHeader[:50]+"...")

	return nil
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func (b *S3HTTPBackend) buildURL(objectKey string) string {
	// Build URL: endpoint/bucket/key
	// endpoint: https://project.storage.supabase.co/storage/v1/s3
	// path: /bucket/key but with any query string handled properly
	if strings.Contains(objectKey, "?") {
		// URL has query parameters - need to append to path properly
		parts := strings.SplitN(objectKey, "?", 2)
		key := parts[0]
		query := parts[1]
		if key != "" {
			return fmt.Sprintf("%s/%s/%s?%s", b.endpoint, b.bucketName, key, query)
		}
		return fmt.Sprintf("%s/%s?%s", b.endpoint, b.bucketName, query)
	}
	return fmt.Sprintf("%s/%s/%s", b.endpoint, b.bucketName, objectKey)
}

func (b *S3HTTPBackend) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	log.Printf("[storage] S3HTTP ListObjects: bucket=%s prefix=%s", b.bucketName, prefix)
	
	// Build URL with query params
	url := b.buildURL("?list-type=2&prefix=" + prefix + "&max-keys=1000")
	log.Printf("[storage] URL: %s", url)
	
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	
	if err := b.signRequest(req); err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		log.Printf("[storage] S3HTTP ListObjects request failed: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	bodyStr := string(body)
	if len(bodyStr) > 500 {
		bodyStr = bodyStr[:500] + "..."
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
	
	// Simple XML parsing
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
		
		// Skip if no key
		if key == "" {
			continue
		}
		
		objects = append(objects, ObjectInfo{Key: key, Size: size})
		
		// Check for more content
		if len(objects) > 100 {
			break
		}
	}

	if objects == nil {
		objects = []ObjectInfo{}
	}
	
	return objects
}

func (b *S3HTTPBackend) ReadObject(ctx context.Context, remotePath string) (io.ReadCloser, error) {
	log.Printf("[storage] S3HTTP ReadObject: %s", remotePath)
	
	url := b.buildURL(remotePath)
	req, err := http.NewRequest("GET", url, nil)
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
	url := b.buildURL(remotePath)
	req, err := http.NewRequest("HEAD", url, nil)
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
	url := b.buildURL(remotePath)
	req, err := http.NewRequest("DELETE", url, nil)
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