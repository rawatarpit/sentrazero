package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3BackendV2 struct {
	client     *s3.Client
	bucketName string
}

func NewS3BackendV2(endpoint, bucketName, region string, creds *S3Credentials) (*S3BackendV2, error) {
	log.Printf("[storage] NewS3BackendV2: endpoint=%s bucket=%s region=%s", endpoint, bucketName, region)

	// Extract base URL from full endpoint
	// endpoint: https://project.storage.supabase.co/storage/v1/s3
	// baseURL: https://project.storage.supabase.co
	baseURL := endpoint
	if strings.HasSuffix(endpoint, "/storage/v1/s3") {
		baseURL = strings.TrimSuffix(endpoint, "/storage/v1/s3")
	}
	log.Printf("[storage] Base URL: %s", baseURL)

	// Create custom endpoint resolver for S3 that returns the full endpoint with custom path
	resolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		log.Printf("[storage] resolver called: service=%s region=%s", service, region)
		if service == "s3" || service == "s3control" {
			return aws.Endpoint{
				URL:               endpoint,  // Full endpoint WITH /storage/v1/s3 path
				PartitionID:       "aws",
				SigningRegion:      region,
				HostnameImmutable: true,
				Source:           aws.EndpointSourceCustom,
			}, nil
		}
		// For other services, use default
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	// Load AWS config with custom resolver
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			creds.AccessKeyID,
			creds.SecretAccessKey,
			creds.SessionToken,
		)),
		config.WithRegion(region),
		config.WithEndpointResolver(resolver),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	
	log.Printf("[storage] Creating S3 client with custom endpoint...")
	
	// Create S3 client
	svc := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	log.Printf("[storage] S3 client ready for bucket: %s", bucketName)

	return &S3BackendV2{
		client:     svc,
		bucketName: bucketName,
	}, nil
}

func (b *S3BackendV2) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	log.Printf("[storage] ListObjects: bucket=%s prefix=%s", b.bucketName, prefix)
	
	result, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &b.bucketName,
		Prefix: &prefix,
		MaxKeys: 1000,
	})
	if err != nil {
		log.Printf("[storage] ListObjects error: %v", err)
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	objects := make([]ObjectInfo, 0, len(result.Contents))
	for _, obj := range result.Contents {
		objects = append(objects, ObjectInfo{
			Key:          *obj.Key,
			Size:         obj.Size,
			LastModified: *obj.LastModified,
		})
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	log.Printf("[storage] ListObjects: found %d objects", len(objects))
	return objects, nil
}

func (b *S3BackendV2) ReadObject(ctx context.Context, remotePath string) (io.ReadCloser, error) {
	obj, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &b.bucketName,
		Key:    &remotePath,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", remotePath, err)
	}
	return obj.Body, nil
}

func (b *S3BackendV2) WriteObject(ctx context.Context, remotePath string, reader io.Reader) error {
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &b.bucketName,
		Key:    &remotePath,
		Body:   reader,
	})
	if err != nil {
		return fmt.Errorf("failed to put object %s: %w", remotePath, err)
	}
	return nil
}

func (b *S3BackendV2) StatObject(ctx context.Context, remotePath string) (ObjectInfo, error) {
	result, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &b.bucketName,
		Key:    &remotePath,
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "404") {
			return ObjectInfo{}, fmt.Errorf("object not found: %s", remotePath)
		}
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Key:          remotePath,
		Size:         result.ContentLength,
		LastModified: *result.LastModified,
	}, nil
}

func (b *S3BackendV2) DeleteObject(ctx context.Context, remotePath string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &b.bucketName,
		Key:    &remotePath,
	})
	return err
}