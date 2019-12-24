package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/crypto/ssh"
)

var (
	configFile string
	bind       string
	debug      bool
)

func init() {
	flag.StringVar(&configFile, "config", "s3-sftp-proxy.toml", "configuration file")
	flag.StringVar(&bind, "bind", "", "listen on addr:port")
	flag.BoolVar(&debug, "debug", false, "turn on debugging output")
}

func buildSSHServerConfig(buckets *S3Buckets, cfg *S3SFTPProxyConfig) (*ssh.ServerConfig, error) {
	pem, err := ioutil.ReadFile(cfg.HostKeyFile)
	if err != nil {
		return nil, errors.Wrapf(err, `failed to open "%s"`, cfg.HostKeyFile)
	}
	key, err := ssh.ParseRawPrivateKey(pem)
	if err != nil {
		return nil, errors.Wrapf(err, `failed to parse host key "%s"`, cfg.HostKeyFile)
	}
	c := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, passwd []byte) (*ssh.Permissions, error) {
			bucket, ok := buckets.UserToBucketMap[c.User()]
			if !ok {
				return nil, fmt.Errorf("unknown user: %s", c.User())
			}
			u := bucket.Users.Lookup(c.User())
			if u.Password != "" && u.Password == string(passwd) {
				return nil, nil
			}
			return nil, fmt.Errorf("passwords do not match")
		},
		PublicKeyCallback: func(c ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			bucket, ok := buckets.UserToBucketMap[c.User()]
			if !ok {
				return nil, fmt.Errorf("unknown user: %s", c.User())
			}
			u := bucket.Users.Lookup(c.User())
			if u.PublicKeys != nil {
				keyMarshaled := key.Marshal()
				for _, herKey := range u.PublicKeys {
					if herKey.Type() == key.Type() && len(herKey.Marshal()) == len(keyMarshaled) && bytes.Compare(herKey.Marshal(), keyMarshaled) == 0 {
						return &ssh.Permissions{
							Extensions: map[string]string{
								"pubkey-fp": ssh.FingerprintSHA256(key),
							},
						}, nil
					}
				}
			}
			return nil, fmt.Errorf("public keys do not match")
		},
		KeyboardInteractiveCallback: func(c ssh.ConnMetadata, client ssh.KeyboardInteractiveChallenge) (*ssh.Permissions, error) {
			bucket, ok := buckets.UserToBucketMap[c.User()]
			if !ok {
				return nil, fmt.Errorf("unknown user: %s", c.User())
			}
			if !bucket.KeyboardInteractiveAuthEnabled {
				return nil, fmt.Errorf("keyboard interactive authentication not enabled")
			}
			u := bucket.Users.Lookup(c.User())
			if u.Password == "" {
				return nil, fmt.Errorf("no credentials are present")
			}
			answers, err := client(u.Name, "", []string{"Password: "}, []bool{false})
			if err != nil {
				return nil, errors.Wrapf(err, "keyboard interactive conversation failed")
			}
			if answers[0] != u.Password {
				return nil, fmt.Errorf("passwords do not match")
			}
			return nil, nil
		},
		BannerCallback: func(c ssh.ConnMetadata) string {
			return cfg.Banner
		},
	}
	sgn, err := ssh.NewSignerFromKey(key)
	if err != nil {
		return nil, err
	}
	c.AddHostKey(sgn)
	return c, nil
}

func bail(msg string, status ...interface{}) {
	os.Stderr.Write([]byte(msg + "\n"))
	statusCode := 1
	if len(status) > 0 {
		var ok bool
		statusCode, ok = status[0].(int)
		if !ok {
			panic("invalid argument for bail()")
		}
	}
	os.Exit(statusCode)
}

func main() {
	flag.Parse()
	cfg, err := ReadConfigFromFile(configFile)
	if err != nil {
		bail(err.Error())
	}

	uStores, err := NewUserStoresFromConfig(cfg)
	if err != nil {
		bail(err.Error())
	}

	buckets, err := NewS3BucketFromConfig(uStores, cfg)
	if err != nil {
		bail(err.Error())
	}

	sCfg, err := buildSSHServerConfig(buckets, cfg)
	if err != nil {
		bail(err.Error())
	}

	_bind := bind
	if _bind == "" {
		_bind = cfg.Bind
		if _bind == "" {
			_bind = ":10022"
		}
	}

	logger := logrus.New()
	if debug {
		logger.SetLevel(logrus.DebugLevel)
	}

	lsnr, err := net.Listen("tcp", _bind)
	if err != nil {
		bail(err.Error())
	}
	defer lsnr.Close()
	logger.Info("Listen on ", _bind)

	metricsBind := cfg.MetricsBind
	if metricsBind == "" {
		metricsBind = ":2112"
	}

	metricsEndpoint := cfg.MetricsEndpoint
	if metricsEndpoint == "" {
		metricsEndpoint = "/metrics"
	}

	http.Handle(metricsEndpoint, promhttp.Handler())

	go func() {
	    http.ListenAndServe(metricsBind, nil)
	}()

    logger.Info("Metrics listen on ", metricsBind, metricsEndpoint)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt)

	errChan := make(chan error)
	go func() {
		errChan <- (&Server{
			S3Buckets:                buckets,
			ServerConfig:             sCfg,
			Log:                      logger,
			ReaderLookbackBufferSize: *cfg.ReaderLookbackBufferSize,
			ReaderMinChunkSize:       *cfg.ReaderMinChunkSize,
			ListerLookbackBufferSize: *cfg.ListerLookbackBufferSize,
			PhantomObjectMap:         NewPhantomObjectMap(),
			Now:                      time.Now,
		}).RunListenerEventLoop(ctx, lsnr.(*net.TCPListener))
	}()

outer:
	for {
		select {
		case err = <-errChan:
			if err != nil {
				bail(err.Error())
			}
			break outer
		case <-sigChan:
			cancel()
		}
	}
}
