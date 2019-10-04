package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/url"
)

var (
	minReaderLookbackBufferSize = 1048576
	minReaderMinChunkSize       = 262144
	minListerLookbackBufferSize = 100
	vTrue                       = true
)

type URL struct {
	*url.URL
}

func (u *URL) UnmarshalText(text []byte) (err error) {
	u.URL, err = url.Parse(string(text))
	return
}

type AWSCredentialsConfig struct {
	AWSAccessKeyID     string `toml:"aws_access_key_id"`
	AWSSecretAccessKey string `toml:"aws_secret_access_key"`
}

type S3BucketConfig struct {
	Profile                        string                   `toml:"profile"`
	Credentials                    *AWSCredentialsConfig    `toml:"credentials"`
	Region                         string                   `toml:"region"`
	Endpoint                       string                   `toml:"endpoint"`
	DisableSSL                     *bool                    `toml:"disable_ssl"`
	S3ForcePathStyle               *bool                    `toml:"s3_force_path_style"`
	Bucket                         string                   `toml:"bucket"`
	KeyPrefix                      string                   `toml:"key_prefix"`
	BucketUrl                      *URL                     `toml:"bucket_url"`
	Auth                           string                   `toml:"auth"`
	MaxObjectSize                  *int64                   `toml:"max_object_size"`
	Readable                       *bool                    `toml:"readble"`
	Writable                       *bool                    `toml:"writable"`
	Listable                       *bool                    `toml:"listable"`
	ServerSideEncryption           ServerSideEncryptionType `toml:"server_side_encryption"`
	SSECustomerKey                 string                   `toml:"sse_customer_key"`
	SSEKMSKeyId                    string                   `toml:"sse_kms_key_id"`
	KeyboardInteractiveAuthEnabled bool                     `toml:"keyboard_interactive_auth"`
}

type AuthUser struct {
	Password      string `toml:"password"`
	PublicKeys    string `toml:"public_keys"`
	PublicKeyFile string `toml:"public_key_file"`
}

type AuthConfig struct {
	Type       string              `toml:"type"`
	UserDBFile string              `toml:"user_db_file"`
	Users      map[string]AuthUser `toml:"users"`
}

type S3SFTPProxyConfig struct {
	Bind                     string                     `toml:"bind"`
	HostKeyFile              string                     `toml:"host_key_file"`
	Banner                   string                     `toml:"banner"`
	ReaderLookbackBufferSize *int                       `toml:"reader_lookback_buffer_size"`
	ReaderMinChunkSize       *int                       `toml:"reader_min_chunk_size"`
	ListerLookbackBufferSize *int                       `toml:"lister_lookback_buffer_size"`
	Buckets                  map[string]*S3BucketConfig `toml:"buckets"`
	AuthConfigs              map[string]*AuthConfig     `toml:"auth"`
	MetricsBind              string                     `toml:"metrics_bind"`
	MetricsEndpoint          string                     `toml:"metrics_endpoint"`
}

func validateAndFixupBucketConfig(bCfg *S3BucketConfig) error {
	if bCfg.Profile != "" {
		if bCfg.Credentials != nil {
			return fmt.Errorf("no credentials may be specified if profile is given")
		}
	}
	if bCfg.BucketUrl != nil {
		if bCfg.Bucket != "" {
			return fmt.Errorf("bucket may not be specified if bucket_url is given")
		}
		if bCfg.KeyPrefix != "" {
			return fmt.Errorf("root path may not be specified if bucket_url is given")
		}
		if bCfg.BucketUrl.Host == "" {
			return fmt.Errorf("bucket name is empty")
		}
		if bCfg.BucketUrl.Scheme != "s3" {
			return fmt.Errorf("bucket URL scheme must be \"s3\"")
		}
		bCfg.Bucket = bCfg.BucketUrl.Host
		bCfg.KeyPrefix = bCfg.BucketUrl.Path
	} else {
		if bCfg.Bucket == "" {
			return fmt.Errorf("bucket name is empty")
		}
	}
	if bCfg.Auth == "" {
		return fmt.Errorf("auth is not specified")
	}
	if bCfg.Readable == nil {
		bCfg.Readable = &vTrue
	}
	if bCfg.Writable == nil {
		bCfg.Writable = &vTrue
	}
	if bCfg.Listable == nil {
		bCfg.Listable = &vTrue
	}
	return nil
}

func validateAndFixupAuthConfigInplace(aCfg *AuthConfig) error {
	if aCfg.UserDBFile != "" {
		return fmt.Errorf(`user_db_file may not be specified when auth type is "inplace"`)
	}
	if aCfg.Users == nil || len(aCfg.Users) == 0 {
		fmt.Printf("%#v\n", aCfg.Users)
		return fmt.Errorf(`no "users" present`)
	}
	return nil
}

func validateAndFixupAuthConfig(aCfg *AuthConfig) error {
	switch aCfg.Type {
	case "inplace":
		return validateAndFixupAuthConfigInplace(aCfg)
	default:
		return fmt.Errorf("unknown auth type: %s", aCfg.Type)
	}
}

func ReadConfig(tomlStr string) (*S3SFTPProxyConfig, error) {
	cfg := &S3SFTPProxyConfig{
		Buckets:     map[string]*S3BucketConfig{},
		AuthConfigs: map[string]*AuthConfig{},
	}

	_, err := toml.Decode(tomlStr, cfg)
	if err != nil {
		return nil, err
	}

	if len(cfg.Buckets) == 0 {
		return nil, fmt.Errorf("no bucket configs are present")
	}

	if len(cfg.AuthConfigs) == 0 {
		return nil, fmt.Errorf("no auth configs are present")
	}

	if cfg.HostKeyFile == "" {
		return nil, fmt.Errorf("no host key file is specified")
	}

	if len(cfg.Banner) > 0 && cfg.Banner[len(cfg.Banner)-1] != '\n' {
		cfg.Banner += "\n"
	}

	if cfg.ReaderLookbackBufferSize == nil {
		cfg.ReaderLookbackBufferSize = &minReaderLookbackBufferSize
	} else if *cfg.ReaderLookbackBufferSize < minReaderLookbackBufferSize {
		return nil, fmt.Errorf("reader_lookback_buffer_size must be equal to or greater than %d", minReaderMinChunkSize)
	}

	if cfg.ReaderMinChunkSize == nil {
		cfg.ReaderMinChunkSize = &minReaderMinChunkSize
	} else if *cfg.ReaderMinChunkSize < minReaderMinChunkSize {
		return nil, fmt.Errorf("reader_min_chunk_size must be equal to or greater than %d", minReaderMinChunkSize)
	}

	if cfg.ListerLookbackBufferSize == nil {
		cfg.ListerLookbackBufferSize = &minListerLookbackBufferSize
	} else if *cfg.ListerLookbackBufferSize < minListerLookbackBufferSize {
		return nil, fmt.Errorf("lister_lookback_buffer_size must be equal to or greater than %d", minListerLookbackBufferSize)
	}

	for name, bCfg := range cfg.Buckets {
		err := validateAndFixupBucketConfig(bCfg)
		if err != nil {
			return nil, errors.Wrapf(err, `bucket config "%s"`, name)
		}
	}

	for name, aCfg := range cfg.AuthConfigs {
		err := validateAndFixupAuthConfig(aCfg)
		if err != nil {
			return nil, errors.Wrapf(err, `auth config "%s"`, name)
		}
	}

	return cfg, err
}

func ReadConfigFromFile(tomlFile string) (*S3SFTPProxyConfig, error) {
	tomlStr, err := ioutil.ReadFile(tomlFile)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open %s", tomlFile)
	}

	cfg, err := ReadConfig(string(tomlStr))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse %s", tomlFile)
	}

	return cfg, nil
}
