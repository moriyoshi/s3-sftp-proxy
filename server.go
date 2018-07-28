package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Server struct {
	*ssh.ServerConfig
	*S3Buckets
	*PhantomObjectMap
	ReaderLookbackBufferSize int
	ReaderMinChunkSize       int
	ListerLookbackBufferSize int
	Log                      interface {
		DebugLogger
		InfoLogger
		ErrorLogger
	}
	Now func() time.Time
}

func asHandlers(handlers interface {
	sftp.FileReader
	sftp.FileWriter
	sftp.FileCmder
	sftp.FileLister
}) sftp.Handlers {
	return sftp.Handlers{handlers, handlers, handlers, handlers}
}

func (s *Server) HandleChannel(ctx context.Context, bucket *S3Bucket, sshCh ssh.Channel, reqs <-chan *ssh.Request) {
	defer s.Log.Debug("HandleChannel ended")
	server := sftp.NewRequestServer(
		sshCh,
		asHandlers(
			&S3BucketIO{
				Bucket: bucket,
				ReaderLookbackBufferSize: s.ReaderLookbackBufferSize,
				ReaderMinChunkSize:       s.ReaderMinChunkSize,
				ListerLookbackBufferSize: s.ListerLookbackBufferSize,
				Log:                  s.Log,
				PhantomObjectMap:     s.PhantomObjectMap,
				Perms:                bucket.Perms,
				ServerSideEncryption: &bucket.ServerSideEncryption,
				Now:                  s.Now,
			},
		),
	)

	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer s.Log.Debug("HandleChannel.discardRequest ended")
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
				ok := false
				if req.Type == "subsystem" && string(req.Payload[4:]) == "sftp" {
					ok = true
				}
				req.Reply(ok, nil)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer s.Log.Debug("HandleChannel.serve ended")
		defer wg.Done()
		defer cancel()
		go func() {
			<-innerCtx.Done()
			server.Close()
		}()
		if err := server.Serve(); err != io.EOF {
			s.Log.Error(err.Error())
		}
	}()

	wg.Wait()
}

func (s *Server) HandleClient(ctx context.Context, conn *net.TCPConn) error {
	defer s.Log.Debug("HandleClient ended")
	defer func() {
		F(s.Log.Info, "connection from client %s closed", conn.RemoteAddr().String())
		conn.Close()
	}()
	F(s.Log.Info, "connected from client %s", conn.RemoteAddr().String())

	// Before use, a handshake must be performed on the incoming net.Conn.
	sconn, chans, reqs, err := ssh.NewServerConn(conn, s.ServerConfig)
	if err != nil {
		return err
	}

	F(s.Log.Info, "user %s logged in", sconn.User())
	bucket, ok := s.UserToBucketMap[sconn.User()]
	if !ok {
		return fmt.Errorf("unknown error: no bucket designated to user %s found", sconn.User())
	}

	wg := sync.WaitGroup{}

	innerCtx, cancel := context.WithCancel(ctx)

	wg.Add(1)
	go func(reqs <-chan *ssh.Request) {
		defer wg.Done()
		defer cancel()
		defer s.Log.Debug("HandleClient.requestHandler ended")
	outer:
		for {
			select {
			case <-innerCtx.Done():
				break outer
			case <-reqs:
				break
			}
		}
	}(reqs)

	wg.Add(1)
	go func(chans <-chan ssh.NewChannel) {
		defer wg.Done()
		defer cancel()
		defer s.Log.Debug("HandleClient.channelHandler ended")
	outer:
		for {
			select {
			case <-innerCtx.Done():
				break outer
			case newSSHCh := <-chans:
				if newSSHCh == nil {
					break outer
				}
				if newSSHCh.ChannelType() != "session" {
					newSSHCh.Reject(ssh.UnknownChannelType, "unknown channel type")
					F(s.Log.Info, "unknown channel type: %s", newSSHCh.ChannelType())
					continue
				}
				F(s.Log.Info, "channel: %s", newSSHCh.ChannelType())

				sshCh, reqs, err := newSSHCh.Accept()
				if err != nil {
					F(s.Log.Error, "could not accept channel", err.Error())
					break outer
				}

				wg.Add(1)
				go func() {
					defer wg.Done()
					s.HandleChannel(ctx, bucket, sshCh, reqs)
				}()
			}
		}
	}(chans)

	wg.Wait()
	return nil
}

func (s *Server) RunListenerEventLoop(ctx context.Context, lsnr *net.TCPListener) error {
	defer s.Log.Debug("RunListenerEventLoop ended")
	connChan := make(chan *net.TCPConn)
	var err error

	go func() {
		defer s.Log.Debug("RunListenerEventLoop.connHandler ended")
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
			if conn == nil {
				break outer
			}
			go func() {
				err := s.HandleClient(ctx, conn)
				if err != nil {
					s.Log.Error(err.Error())
				}
			}()
		case <-ctx.Done():
			lsnr.SetDeadline(time.Unix(1, 0))
			break
		}
	}

	if IsTimeout(err) {
		err = nil
	}

	return err
}
