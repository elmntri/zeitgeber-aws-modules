package bucket_connector

import (
	"context"
	"log"
	"os"
	
	"go.uber.org/fx"
	"go.uber.org/zap"

    "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var logger *zap.Logger

const (
	DefaultAccessKey = "xxx"
	DefaultSecretKey = "yyy"
	DefaultBucket = "aws-s3-test-buckey"
	DefaultRegion = "us-west-1"
)

type UploaderReq struct {
    FileName 	string `json:"fileName"`
    BucketName 	string `json:"bucketName"`
    RawData  	string `json:"rawData"`
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

			// Load the Shared AWS Configuration (~/.aws/config)
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

func (c *BucketConnector) onStart(ctx context.Context) error {
	
	c.logger.Info("BucketConnector onStart")

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

func (c *BucketConnector) UploadFile(filename string, bucketName string, data *os.File, contentType string) (error) {
    _, err := c.client.PutObject(context.TODO(), &s3.PutObjectInput{
        Bucket:      aws.String(bucketName),
        Key:         aws.String(filename),
        Body:        data,
		ContentType: aws.String(contentType), // Set Content-Type header
    })

	return err
}

func (c *BucketConnector) SaveFile(req *UploaderReq) (string, error) {
	return "", nil
	// new a bucket client
	// ctx := context.Background()

	// decode, err := base64.StdEncoding.DecodeString(req.Data)
	// reader := base64.NewDecoder(base64.StdEncoding, strings.NewReader(req.RawData))

	// // init uploder
	// fileName := uuid.New().String()
	// if req.FileName != "" {
	// 	fileName = req.FileName;
	// }

	// filePath := fmt.Sprintf("%s/%s", req.Category, fileName)

	// bucket := c.client.Bucket(viper.GetString(c.getConfigPath("bucket_name")))
	// w := bucket.Object(filePath).NewWriter(ctx)
	// w.ACL = []storage.ACLRule{{Entity: storage.AllUsers, Role: storage.RoleReader}}

	// // upload to bucket
	// if _, err := io.Copy(w, reader); err != nil {
	// 	c.logger.Error("io.Copy Error")
	// 	return "", err
	// }
	// if err := w.Close(); err != nil {
	// 	c.logger.Error("io.Close Error")
	// 	return "", err
	// }

	// u, err := url.Parse(fmt.Sprintf("%v/%v", w.Attrs().Bucket, w.Attrs().Name))
	// if err != nil {
	// 	c.logger.Error("url.Parse Error")
	// 	return "", err
	// }

	// url := fmt.Sprintf("https://%s", u.EscapedPath())

	// return url, nil
}

func (c *BucketConnector) GetClient() *s3.Client {
	return c.client
}