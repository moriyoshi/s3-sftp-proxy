package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
)

// Server SFTP server struct
type Server struct {
	*ssh.ServerConfig
	*S3Buckets
	*PhantomObjectMap
	UploadMemoryBufferPool   *MemoryBufferPool
	ReaderLookbackBufferSize int
	ReaderMinChunkSize       int
	ListerLookbackBufferSize int
	Log                      logrus.FieldLogger
	Now                      func() time.Time
	UploadChan               chan<- *S3PartToUpload
}

// NewServer creates a new sftp server
func NewServer(ctx context.Context, buckets *S3Buckets, serverConfig *ssh.ServerConfig, logger logrus.FieldLogger, readerLookbackBufferSize int, readerMinChunkSize int, listerLookbackBufferSize int, partSize int, uploadMemoryBufferPoolSize int, uploadMemoryBufferPoolTimeout time.Duration, uploadChan chan<- *S3PartToUpload) *Server {
	return &Server{
		S3Buckets:                buckets,
		ServerConfig:             serverConfig,
		Log:                      logger,
		ReaderLookbackBufferSize: readerLookbackBufferSize,
		ReaderMinChunkSize:       readerMinChunkSize,
		ListerLookbackBufferSize: listerLookbackBufferSize,
		UploadMemoryBufferPool:   NewMemoryBufferPool(ctx, partSize, uploadMemoryBufferPoolSize, uploadMemoryBufferPoolTimeout),
		PhantomObjectMap:         NewPhantomObjectMap(),
		Now:                      time.Now,
		UploadChan:               uploadChan,
	}
}

func asHandlers(handlers interface {
	sftp.FileReader
	sftp.FileWriter
	sftp.FileCmder
	sftp.FileLister
}) sftp.Handlers {
	return sftp.Handlers{FileGet: handlers, FilePut: handlers, FileCmd: handlers, FileList: handlers}
}

// HandleChannel handles an sftp channel
func (s *Server) HandleChannel(ctx context.Context, bucket *S3Bucket, sshCh ssh.Channel, reqs <-chan *ssh.Request, userInfo *UserInfo, log logrus.FieldLogger) {
	defer s.Log.Debug("HandleChannel ended")
	server := sftp.NewRequestServer(
		sshCh,
		asHandlers(
			&S3BucketIO{
				Ctx:                      ctx,
				Bucket:                   bucket,
				ReaderLookbackBufferSize: s.ReaderLookbackBufferSize,
				ReaderMinChunkSize:       s.ReaderMinChunkSize,
				ListerLookbackBufferSize: s.ListerLookbackBufferSize,
				UploadMemoryBufferPool:   s.UploadMemoryBufferPool,
				Log:                      log,
				PhantomObjectMap:         s.PhantomObjectMap,
				Perms:                    bucket.Perms,
				ServerSideEncryption:     &bucket.ServerSideEncryption,
				Now:                      s.Now,
				UserInfo:                 userInfo,
				UploadChan:               s.UploadChan,
			},
		),
	)

	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer log.Debug("HandleChannel.discardRequest ended")
		defer wg.Done()
		defer cancel()
	outer:
		for {
			select {
			case <-innerCtx.Done():
				break outer
			case req := <-reqs:
				if req == nil {
					break outer
				}
				ok := req.Type == "subsystem" && string(req.Payload[4:]) == "sftp"
				req.Reply(ok, nil)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer log.Debug("HandleChannel.serve ended")
		defer wg.Done()
		defer cancel()
		go func() {
			<-innerCtx.Done()
			server.Close()
		}()
		if err := server.Serve(); err != io.EOF {
			log.WithField("exception", err).Error("Error on server")
		}
	}()

	wg.Wait()
}

// HandleClient handles a client connection
func (s *Server) HandleClient(ctx context.Context, conn *net.TCPConn) error {
	log := s.Log.WithField("remote_addr", conn.RemoteAddr().String())
	defer log.Debug("HandleClient ended")
	defer func() {
		mUsersConnected.Dec()
		log.Info("Connection from client closed")
		conn.Close()
	}()
	log.Info("Connected from client")

	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-innerCtx.Done()
		conn.SetDeadline(time.Unix(1, 0))
	}()

	// Before use, a handshake must be performed on the incoming net.Conn.
	sconn, chans, reqs, err := ssh.NewServerConn(conn, s.ServerConfig)
	if err != nil {
		return err
	}

	userInfo := &UserInfo{
		Addr: conn.RemoteAddr(),
		User: sconn.User(),
	}

	log = log.WithField("user", sconn.User())
	log.Info("User logged in")
	mUsersConnected.Inc()
	bucket, ok := s.UserToBucketMap[sconn.User()]
	if !ok {
		log.Error("No bucket designated to user")
		return fmt.Errorf("unknown error: no bucket designated to user %s found", sconn.User())
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func(reqs <-chan *ssh.Request) {
		defer wg.Done()
		defer log.Debug("HandleClient.requestHandler ended")
		for range reqs {
		}
	}(reqs)

	wg.Add(1)
	go func(chans <-chan ssh.NewChannel) {
		defer wg.Done()
		defer cancel()
		defer log.Debug("HandleClient.channelHandler ended")
		for newSSHCh := range chans {
			if newSSHCh.ChannelType() != "session" {
				newSSHCh.Reject(ssh.UnknownChannelType, "unknown channel type")
				log.Warnf("Unknown channel type: %s", newSSHCh.ChannelType())
				continue
			}
			log.Infof("Channel: %s", newSSHCh.ChannelType())

			sshCh, reqs, err := newSSHCh.Accept()
			if err != nil {
				log.WithField("exception", err).Error("Could not accept channel")
				break
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				s.HandleChannel(innerCtx, bucket, sshCh, reqs, userInfo, log)
			}()
		}
	}(chans)

	wg.Wait()
	return nil
}

// RunListenerEventLoop runs the listener event loop, which waits for incoming TCP connections
func (s *Server) RunListenerEventLoop(ctx context.Context, lsnr *net.TCPListener) error {
	defer s.Log.Debug("RunListenerEventLoop ended")

	wg := sync.WaitGroup{}
	connChan := make(chan *net.TCPConn)
	var err error

	wg.Add(1)
	go func() {
		defer s.Log.Debug("RunListenerEventLoop.connHandler ended")
		defer wg.Done()
		defer close(connChan)
	outer:
		for {
			var conn *net.TCPConn
			conn, err = lsnr.AcceptTCP()
			if err != nil {
				return
			}
			select {
			case <-ctx.Done():
				conn.Close()
				break outer
			case connChan <- conn:
			}
		}
	}()

outer:
	for {
		select {
		case conn := <-connChan:
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := s.HandleClient(ctx, conn)
				if err != nil {
					s.Log.Error(err.Error())
				}
			}()
		case <-ctx.Done():
			lsnr.SetDeadline(time.Unix(1, 0))
			break outer
		}
	}

	// drain
	for range connChan {
	}

	wg.Wait()

	if IsTimeout(err) {
		err = nil
	}

	return err
}
