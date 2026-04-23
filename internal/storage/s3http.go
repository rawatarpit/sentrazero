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
	endpoint   string
	accessKey string
	secretKey string
	client   *http.Client
}

func NewS3HTTPBackend(endpoint, bucketName, region string, creds *S3Credentials) (*S3HTTPBackend, error) {
	log.Printf("[storage] S3HTTP: endpoint=%s bucket=%s region=%s", endpoint, bucketName, region)

	// Parse endpoint to get host
	// endpoint: https://project.storage.supabase.co/storage/v1/s3
	endpoint = strings.TrimSuffix(endpoint, "/")

	return &S3HTTPBackend{
		bucketName: bucketName,
		endpoint:   endpoint,
		accessKey:  creds.AccessKeyID,
		secretKey:  creds.SecretAccessKey,
		client:     &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (b *S3HTTPBackend) signRequest(req *http.Request, region string) error {
	// AWS Signature Version 4 signing
	now := time.Now().UTC()
	req.Header.Set("X-Amz-Date", now.Format("20060102T150405Z"))
	req.Header.Set("Host", req.URL.Host)

	// Canonical request
	canonicalURI := req.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	
	canonicalQueryString := req.URL.RawQuery
	if canonicalQueryString != "" {
		canonicalQueryString = strings.ReplaceAll(canonicalQueryString, "+", "%20")
	}

	// Canonical headers
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-date:%s\n", 
		req.URL.Host, now.Format("20060102T150405Z"))
	signedHeaders := "host;x-amz-date"

	// Payload hash
	payload := ""
	if req.Body != nil {
		if body, err := io.ReadAll(req.Body); err == nil {
			if len(body) > 0 {
				hash := sha256.Sum256(body)
				payload = hex.EncodeToString(hash[:])
				req.Body = io.NopCloser(strings.NewReader(string(body)))
			}
		}
	}
	if payload == "" {
		payload = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	}
	req.Header.Set("X-Amz-Content-Sha256", payload)

	// Canonical request hash
	cr := req.Method + "\n" + canonicalURI + "\n" + canonicalQueryString + "\n" + canonicalHeaders + "\n" + signedHeaders + "\n" + payload
	hash := sha256.Sum256([]byte(cr))
	canonicalRequestHash := hex.EncodeToString(hash[:])

	// String to sign
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	service := "s3"
	algorithm := "AWS4-HMAC-SHA256"
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s",
		algorithm,
		amzDate,
		credentialScope,
		canonicalRequestHash,
	)

	// Signing key
	kDate := hmacSHA256([]byte("AWS4"+b.secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")

	// Signature
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	// Authorization header
	authHeader := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm, b.accessKey, credentialScope, signedHeaders, signature)
	req.Header.Set("Authorization", authHeader)

	return nil
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func (b *S3HTTPBackend) makeRequest(method, objectKey string) (*http.Request, error) {
	// Build URL: endpoint/bucket/object
	url := fmt.Sprintf("%s/%s/%s", b.endpoint, b.bucketName, objectKey)
	
	// Handle trailing / in endpoint
	if !strings.HasSuffix(b.endpoint, "/") && !strings.HasPrefix(objectKey, "/") {
		url = b.endpoint + "/" + b.bucketName + "/" + objectKey
	} else if strings.HasSuffix(b.endpoint, "/") {
		url = b.endpoint + b.bucketName + "/" + objectKey
	}

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	// Sign the request
	if err := b.signRequest(req, "ap-south-1"); err != nil {
		return nil, err
	}

	return req, nil
}

func (b *S3HTTPBackend) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	log.Printf("[storage] S3HTTP ListObjects: bucket=%s prefix=%s", b.bucketName, prefix)
	
	// Use list objects v2 API
	objectKey := fmt.Sprintf("?list-type=2&prefix=%s&max-keys=1000", prefix)
	
	url := fmt.Sprintf("%s/%s/%s", b.endpoint, b.bucketName, objectKey)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	
	// Sign
	if err := b.signRequest(req, "ap-south-1"); err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("S3 HTTP %d: %s", resp.StatusCode, string(body))
	}

	// Parse XML response
	return parseListObjectsV2Response(string(body))
}

func parseListObjectsV2Response(xmlStr string) ([]ObjectInfo, error) {
	// Simple XML parsing
	var objects []ObjectInfo
	
	// Extract contents between <Contents> tags
	start := strings.Index(xmlStr, "<Contents>")
	for start >= 0 {
		end := strings.Index(xmlStr[start:], "</Contents>")
		if end < 0 {
			break
		}
		contents := xmlStr[start:start+end+len("</Contents>")]
		
		// Extract key
		keyStart := strings.Index(contents, "<Key>")
		keyEnd := strings.Index(contents[keyStart:], "</Key>")
		if keyStart >= 0 && keyEnd > 0 {
			key := contents[keyStart+len("<Key>"):keyStart+keyEnd]
			
			// Extract size
			sizeStart := strings.Index(contents, "<Size>")
			sizeEnd := strings.Index(contents[sizeStart:], "</Size>")
			var size int64
			if sizeStart >= 0 && sizeEnd > 0 {
				fmt.Sscanf(contents[sizeStart+len("<Size>"):sizeStart+sizeEnd], "%d", &size)
			}
			
			objects = append(objects, ObjectInfo{Key: key, Size: size})
		}
		
		start = strings.Index(xmlStr[start+end+len("</Contents>"):], "<Contents>")
		if start >= 0 {
			start = start + end + len("</Contents>")
		}
	}

	if objects == nil {
		objects = []ObjectInfo{}
	}
	
	log.Printf("[storage] S3HTTP found %d objects", len(objects))
	return objects, nil
}

func (b *S3HTTPBackend) ReadObject(ctx context.Context, remotePath string) (io.ReadCloser, error) {
	log.Printf("[storage] S3HTTP ReadObject: %s", remotePath)
	
	req, err := b.makeRequest("GET", remotePath)
	if err != nil {
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
	// HEAD object
	url := fmt.Sprintf("%s/%s/%s", b.endpoint, b.bucketName, remotePath)
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return ObjectInfo{}, err
	}
	
	if err := b.signRequest(req, "ap-south-1"); err != nil {
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

	size := resp.ContentLength
	return ObjectInfo{Key: remotePath, Size: size}, nil
}

func (b *S3HTTPBackend) DeleteObject(ctx context.Context, remotePath string) error {
	req, err := b.makeRequest("DELETE", remotePath)
	if err != nil {
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