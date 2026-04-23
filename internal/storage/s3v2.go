package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3BackendV2 struct {
	client     *s3.Client
	bucketName string
}

func NewS3BackendV2(endpoint, bucketName, region string, creds *S3Credentials) (*S3BackendV2, error) {
	// Supabase Storage S3 endpoint without /storage/v1/s3
	s3Endpoint := endpoint
	if strings.HasSuffix(endpoint, "/storage/v1/s3") {
		s3Endpoint = strings.TrimSuffix(endpoint, "/storage/v1/s3")
	}

	// Create custom endpoint resolver to override the default S3 endpoint
	resolver := awsv2.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (awsv2.Endpoint, error) {
			return awsv2.Endpoint{
				URL:               "https://" + s3Endpoint,
				HostnameImmutable: true,
				PartitionID:       "aws",
			}, nil
		},
	)

	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			creds.AccessKeyID,
			creds.SecretAccessKey,
			creds.SessionToken,
		)),
		config.WithRegion(region),
		config.WithEndpointResolverWithOptions(resolver),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Force path-style for Supabase Storage
	svc := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	log.Printf("[storage] S3 (AWS SDK v2): connecting to %s with bucket %s", s3Endpoint, bucketName)

	return &S3BackendV2{
		client:     svc,
		bucketName: bucketName,
	}, nil
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

func (b *S3BackendV2) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	result, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &b.bucketName,
		Prefix:    &prefix,
		MaxKeys:    1000,
	})
	if err != nil {
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

	return objects, nil
}

func (b *S3BackendV2) StatObject(ctx context.Context, remotePath string) (ObjectInfo, error) {
	result, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &b.bucketName,
		Key:    &remotePath,
	})
	if err != nil {
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