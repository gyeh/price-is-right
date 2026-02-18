package cloud

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gyeh/npi-rates/internal/mrf"
)

// S3Client wraps S3 operations for result upload/download.
type S3Client struct {
	client *s3.Client
	bucket string
}

// NewS3Client creates an S3 client for the given bucket.
func NewS3Client(ctx context.Context, bucket, region string) (*S3Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	return &S3Client{
		client: s3.NewFromConfig(cfg),
		bucket: bucket,
	}, nil
}

// UploadResults uploads rate results as JSON to S3.
func (c *S3Client) UploadResults(ctx context.Context, key string, results []mrf.RateResult) error {
	data, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("marshaling results: %w", err)
	}

	tmpFile, err := os.CreateTemp("", "s3-upload-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return err
	}
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		tmpFile.Close()
		return err
	}

	_, err = c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        tmpFile,
		ContentType: aws.String("application/json"),
	})
	tmpFile.Close()

	return err
}

// DownloadResults downloads rate results from S3.
func (c *S3Client) DownloadResults(ctx context.Context, key string) ([]mrf.RateResult, error) {
	resp, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("getting S3 object %s: %w", key, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var results []mrf.RateResult
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, fmt.Errorf("unmarshaling results: %w", err)
	}

	return results, nil
}

// UploadBytes uploads raw bytes to S3.
func (c *S3Client) UploadBytes(ctx context.Context, key string, data []byte, contentType string) error {
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	return err
}

// DownloadBytes downloads raw bytes from S3.
func (c *S3Client) DownloadBytes(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("getting S3 object %s: %w", key, err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// DownloadSearchOutput downloads and parses a SearchOutput JSON from S3.
func (c *S3Client) DownloadSearchOutput(ctx context.Context, key string) (*mrf.SearchOutput, error) {
	data, err := c.DownloadBytes(ctx, key)
	if err != nil {
		return nil, err
	}
	var out mrf.SearchOutput
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("parsing search output from %s: %w", key, err)
	}
	return &out, nil
}

// DeleteObject deletes a single object from S3.
func (c *S3Client) DeleteObject(ctx context.Context, key string) error {
	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	return err
}

// ParseS3URI parses an s3://bucket/key URI into bucket and key components.
func ParseS3URI(uri string) (bucket, key string, err error) {
	if !strings.HasPrefix(uri, "s3://") {
		return "", "", fmt.Errorf("invalid S3 URI (must start with s3://): %s", uri)
	}
	rest := uri[5:]
	idx := strings.IndexByte(rest, '/')
	if idx < 0 {
		return "", "", fmt.Errorf("invalid S3 URI (no key): %s", uri)
	}
	return rest[:idx], rest[idx+1:], nil
}
