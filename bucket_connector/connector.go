package bucket_connector

import (
	"context"
	"log"
	"fmt"
	"encoding/base64"
	"net/url"
	"bytes"
	"strings"
	"io"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/spf13/viper"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
)

var logger *zap.Logger

const (
	DefaultBucketName   = "example.com"
	DefaultBucketKey    = "ABCDE"
	DefaultBucketSecret = "example_secret"
	DefaultBucketToken  = ""
	DefaultBucketRegion = "us-west-1"
)

type UploaderReq struct {
    FileName string `json:"file_name"`
    Category string `json:"category"`
    RawData  string `json:"rowData"`
}

type BucketConnector struct {
	params Params
	logger *zap.Logger
	client *s3.Client
	scope  string
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var m *BucketConnector

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *BucketConnector {

			logger = p.Logger.Named(scope)

			cfg, err := config.LoadDefaultConfig(context.TODO())
			if err != nil {
				log.Fatal(err)
				return &BucketConnector{}
			}


			m := &BucketConnector{
				params: p,
				logger: logger,
				scope:  scope,
				client: s3.NewFromConfig(cfg),
			}

			return m
		}),
		fx.Populate(&m),
		fx.Invoke(func(p Params) *BucketConnector {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: m.onStart,
					OnStop:  m.onStop,
				},
			)

			return m
		}),
	)
}

func (c *BucketConnector) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", c.scope, key)
}

func (c *BucketConnector) initDefaultConfigs() {
	viper.SetDefault(c.getConfigPath("bucket_name"), DefaultBucketName)
	viper.SetDefault(c.getConfigPath("bucket_key"), DefaultBucketKey)
	viper.SetDefault(c.getConfigPath("bucket_secret"), DefaultBucketSecret)
	viper.SetDefault(c.getConfigPath("bucket_token"), DefaultBucketToken)
	viper.SetDefault(c.getConfigPath("bucket_region"), DefaultBucketRegion)
}

func (c *BucketConnector) onStart(ctx context.Context) error {
	logger.Info("Starting BucketConnector",
		zap.String("bucket_name", viper.GetString(c.getConfigPath("bucket_name"))),
		zap.String("bucket_key", viper.GetString(c.getConfigPath("bucket_key"))),
		zap.String("bucket_secret", viper.GetString(c.getConfigPath("bucket_secret"))),
		zap.String("bucket_token", viper.GetString(c.getConfigPath("bucket_token"))),
		zap.String("bucket_region", viper.GetString(c.getConfigPath("bucket_region"))),
	)

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			viper.GetString(c.getConfigPath("bucket_key")),
			viper.GetString(c.getConfigPath("bucket_secret")),
			viper.GetString(c.getConfigPath("bucket_token")),
		)),
		config.WithRegion(viper.GetString(c.getConfigPath("bucket_region"))),
	)
	if err != nil {
		c.logger.Error("Load AWS config error", zap.Error(err))
		return err
	}

	c.client = s3.NewFromConfig(cfg)

	return nil
}

func (c *BucketConnector) onStop(ctx context.Context) error {

	c.logger.Info("Stopped BucketConnector")

	return nil
}

func (c *BucketConnector) ListBuckets() ([]types.Bucket, error) {
	result, err := c.client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})

	if err != nil {
		return nil, err
	}

	return result.Buckets, nil
}

func (c *BucketConnector) SaveFile(req *UploaderReq, contentType string) (string, error) {
	decodedData, err := base64.StdEncoding.DecodeString(req.RawData)
	if err != nil {
		c.logger.Error("Upload to S3 error", zap.Error(err))
		return "", err
    }

	reader := bytes.NewReader(decodedData)

	fileName := uuid.New().String()
	if req.FileName != "" {
		fileName = req.FileName
	}

	filePath := fmt.Sprintf("%s/%s", req.Category, fileName)

	c.logger.Info("Uploading file to S3", zap.String("file_path", filePath))

	bucketName := viper.GetString(c.getConfigPath("bucket_name"))
	_, err = c.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(filePath),
		Body:          reader,
		ContentType:   aws.String(contentType),
		ContentLength: aws.Int64(int64(len(decodedData))),
	})
	if err != nil {
		c.logger.Error("Upload to S3 error", zap.Error(err))
		return "", err
	}

	url := fmt.Sprintf("https://%s/%s", bucketName, url.PathEscape(filePath))

	return url, nil
}

func (c *BucketConnector) GetClient() *s3.Client {
	return c.client
}

func (c *BucketConnector) DeleteFileWithPrefix(filePath string) error {
	bucketName := viper.GetString(c.getConfigPath("bucket_name"))

	// List and delete objects with the prefix
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(filePath),
	}

	paginator := s3.NewListObjectsV2Paginator(c.client, listInput)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			c.logger.Error("Failed to list objects", zap.Error(err))
			return err
		}

		// Delete all objects found in this page
		for _, obj := range page.Contents {
			deleteInput := &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    obj.Key,
			}

			_, err := c.client.DeleteObject(context.TODO(), deleteInput)
			if err != nil {
				c.logger.Error("Failed to delete object",
					zap.String("key", *obj.Key),
					zap.Error(err))
				return err
			}

			c.logger.Info("Deleted object",
				zap.String("key", *obj.Key),
				zap.String("bucket", bucketName))
		}
	}

	return nil
}

func (c *BucketConnector) DeleteFile(filePath string) error {
	bucketName := viper.GetString(c.getConfigPath("bucket_name"))

	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(filePath),
	}

	_, err := c.client.DeleteObject(context.TODO(), deleteInput)
	if err != nil {
		// Check if it's a "not found" error
		var noe *types.NotFound
		if !strings.Contains(err.Error(), "NotFound") && !strings.Contains(err.Error(), "NoSuchKey") {
			c.logger.Error("Failed to delete object",
				zap.String("key", filePath),
				zap.Error(err))
			return err
		}
		// If object doesn't exist, treat as success
		return nil
	}

	c.logger.Info("Deleted object",
		zap.String("key", filePath),
		zap.String("bucket", bucketName))

	return nil
}

func (c *BucketConnector) WriteAsFile(filePath string, content []byte) (string, error) {
	bucketName := viper.GetString(c.getConfigPath("bucket_name"))

	reader := bytes.NewReader(content)

	putInput := &s3.PutObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(filePath),
		Body:          reader,
		ContentLength: aws.Int64(int64(len(content))),
		ACL:          types.ObjectCannedACLPublicRead,
	}

	_, err := c.client.PutObject(context.TODO(), putInput)
	if err != nil {
		c.logger.Error("Failed to write object",
			zap.String("key", filePath),
			zap.Error(err))
		return "", err
	}

	c.logger.Info("Wrote object to S3",
		zap.String("key", filePath),
		zap.String("bucket", bucketName),
		zap.Int("size", len(content)))

	url := fmt.Sprintf("https://%s/%s", bucketName, url.PathEscape(filePath))

	return url, nil
}
