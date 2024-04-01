package bucket_connector

import (
	"context"
	"log"
	"fmt"
	"encoding/base64"
	"strings"
	"net/url"

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

	cfg, err := config.LoadDefaultConfig(ctx,
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

func (c *BucketConnector) SaveFile(req *UploaderReq) (string, error) {
	ctx := context.Background()

	encodedData := req.RawData
	reader := base64.NewDecoder(base64.StdEncoding, strings.NewReader(encodedData))

	fileName := uuid.New().String()
	if req.FileName != "" {
		fileName = req.FileName
	}

	filePath := fmt.Sprintf("%s/%s", req.Category, fileName)

	contentType := "image/jpeg"

	// Calculate the approximate content length from the base64 encoded data
	// Every 4 base64 characters represent 3 bytes of data
	rawLength := int64(len(encodedData))
	contentLength := (rawLength / 4) * 3
	if rawLength%4 != 0 { // Handle any padding
		contentLength += rawLength%4 - 1
	}

	c.logger.Info("Uploading file to S3", zap.String("file_path", filePath))

	bucketName := viper.GetString(c.getConfigPath("bucket_name"))
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &bucketName,
		Key:           aws.String(filePath),
		Body:          reader,
		ContentType:   &contentType,
		ContentLength: &contentLength,
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
