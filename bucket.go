package main

import (
	"crypto"
	"encoding/base64"
	"fmt"
	"strings"

	aws "github.com/aws/aws-sdk-go/aws"
	aws_creds "github.com/aws/aws-sdk-go/aws/credentials"
	aws_ec2_role_creds "github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	aws_ec2_meta "github.com/aws/aws-sdk-go/aws/ec2metadata"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

type ServerSideEncryptionType int

const (
	ServerSideEncryptionTypeNone = iota
	ServerSideEncryptionTypeAES256
	ServerSideEncryptionTypeKMS
)

var sseNameToEnumMap = map[string]ServerSideEncryptionType{
	"":       ServerSideEncryptionTypeNone,
	"none":   ServerSideEncryptionTypeNone,
	"aes256": ServerSideEncryptionTypeAES256,
	"kms":    ServerSideEncryptionTypeKMS,
}

func (v *ServerSideEncryptionType) UnmarshalText(text []byte) error {
	_v, ok := sseNameToEnumMap[strings.ToLower(string(text))]
	if !ok {
		return fmt.Errorf("invalid value for ServerSideEncryption: %s", string(text))
	}
	*v = _v
	return nil
}

type ServerSideEncryptionConfig struct {
	Type           ServerSideEncryptionType
	CustomerKey    string
	CustomerKeyMD5 string
	KMSKeyId       string
}

func (cfg *ServerSideEncryptionConfig) CustomerAlgorithm() string {
	if cfg.Type == ServerSideEncryptionTypeAES256 {
		return "AES256"
	} else {
		return ""
	}
}

type Perms struct {
	Readable bool
	Writable bool
	Listable bool
}

type S3Bucket struct {
	Name                 string
	AWSConfig            *aws.Config
	Bucket               string
	KeyPrefix            Path
	MaxObjectSize        int64
	Users                UserStore
	Perms                Perms
	ServerSideEncryption ServerSideEncryptionConfig
}

type S3Buckets struct {
	Buckets         map[string]*S3Bucket
	UserToBucketMap map[string]*S3Bucket
}

func (s3bs *S3Buckets) Get(name string) *S3Bucket {
	b, _ := s3bs.Buckets[name]
	return b
}

func (s3b *S3Bucket) S3(sess *aws_session.Session) *s3.S3 {
	awsCfg := s3b.AWSConfig
	if awsCfg.Credentials == nil {
		awsCfg = s3b.AWSConfig.WithCredentials(aws_creds.NewChainCredentials(
			[]aws_creds.Provider{
				&aws_ec2_role_creds.EC2RoleProvider{
					Client:       aws_ec2_meta.New(sess),
					ExpiryWindow: 0,
				},
				&aws_creds.EnvProvider{},
			},
		))
	}
	return s3.New(sess, awsCfg)
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
			KMSKeyId:       bCfg.SSEKMSKeyId,
		},
	}, nil
}

func NewS3BucketFromConfig(uStores UserStores, cfg *S3SFTPProxyConfig) (*S3Buckets, error) {
	buckets := map[string]*S3Bucket{}
	userToBucketMap := map[string]*S3Bucket{}
	for name, bCfg := range cfg.Buckets {
		bucket, err := buildS3Bucket(uStores, name, bCfg)
		if err != nil {
			return nil, errors.Wrapf(err, "bucket config %s", name)
		}
		for _, user := range bucket.Users.Users {
			_bucket, ok := userToBucketMap[user.Name]
			if ok {
				return nil, fmt.Errorf(`bucket config %s: user "%s" is already assigned to bucket config "%s"`, name, user.Name, _bucket.Name)
			}
			userToBucketMap[user.Name] = bucket
		}
		buckets[name] = bucket
	}
	return &S3Buckets{
		Buckets:         buckets,
		UserToBucketMap: userToBucketMap,
	}, nil
}
