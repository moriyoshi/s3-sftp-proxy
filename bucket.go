package main

import (
	"crypto"
	"encoding/base64"
	"fmt"
	"strings"

	aws "github.com/aws/aws-sdk-go/aws"
	aws_creds "github.com/aws/aws-sdk-go/aws/credentials"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

// ServerSideEncryptionType server side encryption type
type ServerSideEncryptionType int

const (
	// ServerSideEncryptionTypeNone do not use server side encryption on S3
	ServerSideEncryptionTypeNone = iota

	// ServerSideEncryptionTypeAES256 use AES256 encryption type
	ServerSideEncryptionTypeAES256

	// ServerSideEncryptionTypeKMS use KMS as encryption
	ServerSideEncryptionTypeKMS
)

var sseNameToEnumMap = map[string]ServerSideEncryptionType{
	"":       ServerSideEncryptionTypeNone,
	"none":   ServerSideEncryptionTypeNone,
	"aes256": ServerSideEncryptionTypeAES256,
	"kms":    ServerSideEncryptionTypeKMS,
}

// UnmarshalText used to get the server side encryption type from a text present on configuration
func (v *ServerSideEncryptionType) UnmarshalText(text []byte) error {
	_v, ok := sseNameToEnumMap[strings.ToLower(string(text))]
	if !ok {
		return fmt.Errorf("invalid value for ServerSideEncryption: %s", string(text))
	}
	*v = _v
	return nil
}

// ServerSideEncryptionConfig server side encryption configuration
type ServerSideEncryptionConfig struct {
	Type           ServerSideEncryptionType
	CustomerKey    string
	CustomerKeyMD5 string
	KMSKeyID       string
}

// CustomerAlgorithm customer algorithm server side encryption configuration
func (cfg *ServerSideEncryptionConfig) CustomerAlgorithm() string {
	if cfg.Type == ServerSideEncryptionTypeAES256 {
		return "AES256"
	}
	return ""
}

// Perms permissions
type Perms struct {
	Readable bool
	Writable bool
	Listable bool
}

// S3Bucket S3 bucket
type S3Bucket struct {
	Name                           string
	AWSConfig                      *aws.Config
	Bucket                         string
	KeyPrefix                      Path
	MaxObjectSize                  int64
	Users                          UserStore
	Perms                          Perms
	ServerSideEncryption           ServerSideEncryptionConfig
	KeyboardInteractiveAuthEnabled bool
}

// S3Buckets S3 buckets
type S3Buckets struct {
	Buckets         map[string]*S3Bucket
	UserToBucketMap map[string]*S3Bucket
}

// Get gets an S3 bucket given its name
func (s3bs *S3Buckets) Get(name string) *S3Bucket {
	b, _ := s3bs.Buckets[name]
	return b
}

// S3 creates a new instance of S3 client
func (s3b *S3Bucket) S3() (*s3.S3, error) {
	awsCfg := s3b.AWSConfig
	var sess *aws_session.Session
	var err error

	if awsCfg.Credentials == nil {
		sess, err = aws_session.NewSessionWithOptions(aws_session.Options{
			SharedConfigState: aws_session.SharedConfigEnable,
			Config:            *awsCfg,
		})
	} else {
		sess, err = aws_session.NewSession(awsCfg)
	}
	if err != nil {
		return nil, err
	}
	return s3.New(sess), nil
}

func buildS3Bucket(uStores UserStores, name string, bCfg *S3BucketConfig) (*S3Bucket, error) {
	awsCfg := aws.NewConfig()
	if bCfg.Credentials != nil {
		awsCfg = awsCfg.WithCredentials(
			aws_creds.NewStaticCredentials(
				bCfg.Credentials.AWSAccessKeyID,
				bCfg.Credentials.AWSSecretAccessKey,
				"",
			),
		)
	} else if bCfg.Profile != "" {
		awsCfg = awsCfg.WithCredentials(
			aws_creds.NewSharedCredentials(
				"", // TODO: assumes default
				bCfg.Profile,
			),
		)
	} else {
		// credentials are retrieved through EC2 metadata on runtime
	}
	if bCfg.Endpoint != "" {
		awsCfg = awsCfg.WithEndpoint(bCfg.Endpoint)
	}
	if bCfg.S3ForcePathStyle != nil {
		awsCfg = awsCfg.WithS3ForcePathStyle(*bCfg.S3ForcePathStyle)
	}
	if bCfg.DisableSSL != nil {
		awsCfg = awsCfg.WithDisableSSL(*bCfg.DisableSSL)
	}
	if bCfg.Region != "" {
		awsCfg = awsCfg.WithRegion(bCfg.Region)
	}
	users, ok := uStores[bCfg.Auth]
	if !ok {
		return nil, fmt.Errorf("no such auth config: %s", bCfg.Auth)
	}
	keyPrefix := SplitIntoPath(bCfg.KeyPrefix)
	if len(keyPrefix) > 0 && keyPrefix[0] == "" {
		keyPrefix = keyPrefix[1:]
	}
	maxObjectSize := int64(-1)
	if bCfg.MaxObjectSize != nil {
		maxObjectSize = *bCfg.MaxObjectSize
	}

	var customerKey []byte
	var customerKeyMD5 string
	if bCfg.SSECustomerKey != "" {
		var err error
		customerKey, err = base64.StdEncoding.DecodeString(bCfg.SSECustomerKey)
		if err != nil {
			return nil, errors.Wrapf(err, `invalid base64-encoded string specified for "sse_customer_key"`)
		}
		hasher := crypto.MD5.New()
		hasher.Write(customerKey)
		customerKeyMD5 = base64.StdEncoding.EncodeToString(hasher.Sum([]byte{}))
	} else {
		customerKey = []byte{}
	}
	return &S3Bucket{
		Name:          name,
		AWSConfig:     awsCfg,
		Bucket:        bCfg.Bucket,
		KeyPrefix:     keyPrefix,
		MaxObjectSize: maxObjectSize,
		Users:         users,
		Perms: Perms{
			Readable: *bCfg.Readable,
			Writable: *bCfg.Writable,
			Listable: *bCfg.Listable,
		},
		ServerSideEncryption: ServerSideEncryptionConfig{
			Type:           bCfg.ServerSideEncryption,
			CustomerKey:    string(customerKey),
			CustomerKeyMD5: customerKeyMD5,
			KMSKeyID:       bCfg.SSEKMSKeyID,
		},
		KeyboardInteractiveAuthEnabled: bCfg.KeyboardInteractiveAuthEnabled,
	}, nil
}

// NewS3BucketFromConfig creates an S3Buckets from configuration
func NewS3BucketFromConfig(uStores UserStores, cfg *S3SFTPProxyConfig) (*S3Buckets, error) {
	buckets := map[string]*S3Bucket{}
	userToBucketMap := map[string]*S3Bucket{}
	for name, bCfg := range cfg.Buckets {
		bucket, err := buildS3Bucket(uStores, name, bCfg)
		if err != nil {
			return nil, errors.Wrapf(err, "bucket config %s", name)
		}
		for _, user := range bucket.Users.Users {
			_bucket, ok := userToBucketMap[user.GetName()]
			if ok {
				return nil, fmt.Errorf(`bucket config %s: user "%s" is already assigned to bucket config "%s"`, name, user.GetName(), _bucket.Name)
			}
			userToBucketMap[user.GetName()] = bucket
		}
		buckets[name] = bucket
	}
	return &S3Buckets{
		Buckets:         buckets,
		UserToBucketMap: userToBucketMap,
	}, nil
}
