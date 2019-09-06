package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	aws "github.com/aws/aws-sdk-go/aws"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/sftp"
	"github.com/prometheus/client_golang/prometheus"
	// s3crypto "github.com/aws/aws-sdk-go/service/s3/s3crypto"
)

var aclPrivate = "private"

type ReadDeadlineSettable interface {
	SetReadDeadline(t time.Time) error
}

type WriteDeadlineSettable interface {
	SetWriteDeadline(t time.Time) error
}

var sseTypes = map[ServerSideEncryptionType]*string{
	ServerSideEncryptionTypeKMS: aws.String("aws:kms"),
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	} else {
		return &s
	}
}

type S3GetObjectOutputReader struct {
	Ctx          context.Context
	Goo          *aws_s3.GetObjectOutput
	Log          DebugLogger
	Lookback     int
	MinChunkSize int
	mtx          sync.Mutex
	spooled      []byte
	spoolOffset  int
	noMore       bool
}

func (oor *S3GetObjectOutputReader) Close() error {
	if oor.Goo.Body != nil {
		oor.Goo.Body.Close()
		oor.Goo.Body = nil
	}
	return nil
}

func (oor *S3GetObjectOutputReader) ReadAt(buf []byte, off int64) (int, error) {
	oor.mtx.Lock()
	defer oor.mtx.Unlock()

	F(oor.Log.Debug, "len(buf)=%d, off=%d", len(buf), off)
	_o, err := castInt64ToInt(off)
	if err != nil {
		return 0, err
	}
	if _o < oor.spoolOffset {
		return 0, fmt.Errorf("supplied position is out of range")
	}

	s := _o - oor.spoolOffset
	i := 0
	r := len(buf)
	if s < len(oor.spooled) {
		// n = max(r, len(oor.spooled)-s)
		n := r
		if n > len(oor.spooled)-s {
			n = len(oor.spooled) - s
		}
		copy(buf[i:i+n], oor.spooled[s:s+n])
		i += n
		s += n
		r -= n
	}
	if r == 0 {
		return i, nil
	}

	if oor.noMore {
		if i == 0 {
			return 0, io.EOF
		} else {
			return i, nil
		}
	}

	F(oor.Log.Debug, "s=%d, len(oor.spooled)=%d, oor.Lookback=%d", s, len(oor.spooled), oor.Lookback)
	if s <= len(oor.spooled) && s >= oor.Lookback {
		oor.spooled = oor.spooled[s-oor.Lookback:]
		oor.spoolOffset += s - oor.Lookback
		s = oor.Lookback
	}

	var e int
	if len(oor.spooled)+oor.MinChunkSize < s+r {
		e = s + r
	} else {
		e = len(oor.spooled) + oor.MinChunkSize
	}

	if cap(oor.spooled) < e {
		spooled := make([]byte, len(oor.spooled), e)
		copy(spooled, oor.spooled)
		oor.spooled = spooled
	}

	type readResult struct {
		n   int
		err error
	}

	resultChan := make(chan readResult)
	go func() {
		n, err := io.ReadFull(oor.Goo.Body, oor.spooled[len(oor.spooled):e])
		resultChan <- readResult{n, err}
	}()
	select {
	case <-oor.Ctx.Done():
		oor.Goo.Body.(ReadDeadlineSettable).SetReadDeadline(time.Unix(1, 0))
		oor.Log.Debug("canceled")
		return 0, fmt.Errorf("read operation canceled")
	case res := <-resultChan:
		if IsEOF(res.err) {
			oor.noMore = true
		}
		e = len(oor.spooled) + res.n
		oor.spooled = oor.spooled[:e]
		if s < e {
			be := e
			if be > s+r {
				be = s + r
			}
			copy(buf[i:], oor.spooled[s:be])
			return be - s, nil
		} else {
			return 0, io.EOF
		}
	}
}

type S3PutObjectWriter struct {
	Ctx                  context.Context
	Bucket               string
	Key                  Path
	S3                   *aws_s3.S3
	ServerSideEncryption *ServerSideEncryptionConfig
	Log                  interface {
		DebugLogger
		ErrorLogger
	}
	MaxObjectSize    int64
	Info             *PhantomObjectInfo
	PhantomObjectMap *PhantomObjectMap
	mtx              sync.Mutex
	writer           *BytesWriter
}

func (oow *S3PutObjectWriter) Close() error {
	F(oow.Log.Debug, "S3PutObjectWriter.Close")
	oow.mtx.Lock()
	defer oow.mtx.Unlock()
	phInfo := oow.Info.GetOne()
	oow.PhantomObjectMap.RemoveByInfoPtr(oow.Info)
	key := phInfo.Key.String()
	sse := oow.ServerSideEncryption
	F(oow.Log.Debug, "PutObject(Bucket=%s, Key=%s, Sse=%v)", oow.Bucket, key, sse)
	_, err := oow.S3.PutObject(
		&aws_s3.PutObjectInput{
			ACL:                  &aclPrivate,
			Body:                 bytes.NewReader(oow.writer.Bytes()),
			Bucket:               &oow.Bucket,
			Key:                  &key,
			ServerSideEncryption: sseTypes[sse.Type],
			SSECustomerAlgorithm: nilIfEmpty(sse.CustomerAlgorithm()),
			SSECustomerKey:       nilIfEmpty(sse.CustomerKey),
			SSECustomerKeyMD5:    nilIfEmpty(sse.CustomerKeyMD5),
			SSEKMSKeyId:          nilIfEmpty(sse.KMSKeyId),
		},
	)
	if err != nil {
		oow.Log.Debug("=> ", err)
		F(oow.Log.Error, "failed to put object: %s", err.Error())
	} else {
		oow.Log.Debug("=> OK")
	}
	return nil
}

func (oow *S3PutObjectWriter) WriteAt(buf []byte, off int64) (int, error) {
	oow.mtx.Lock()
	defer oow.mtx.Unlock()
	if oow.MaxObjectSize >= 0 {
		if int64(len(buf))+off > oow.MaxObjectSize {
			return 0, fmt.Errorf("file too large: maximum allowed size is %d bytes", oow.MaxObjectSize)
		}
	}
	F(oow.Log.Debug, "len(buf)=%d, off=%d", len(buf), off)
	n, err := oow.writer.WriteAt(buf, off)
	oow.Info.SetSize(oow.writer.Size())
	return n, err
}

type ObjectFileInfo struct {
	_Name         string
	_LastModified time.Time
	_Size         int64
	_Mode         os.FileMode
}

func (ofi *ObjectFileInfo) Name() string {
	return ofi._Name
}

func (ofi *ObjectFileInfo) ModTime() time.Time {
	return ofi._LastModified
}

func (ofi *ObjectFileInfo) Size() int64 {
	return ofi._Size
}

func (ofi *ObjectFileInfo) Mode() os.FileMode {
	return ofi._Mode
}

func (ofi *ObjectFileInfo) IsDir() bool {
	return (ofi._Mode & os.ModeDir) != 0
}

func (ofi *ObjectFileInfo) Sys() interface{} {
	return BuildFakeFileInfoSys()
}

type S3ObjectLister struct {
	DebugLogger
	Ctx              context.Context
	Bucket           string
	Prefix           Path
	S3               *aws_s3.S3
	Lookback         int
	PhantomObjectMap *PhantomObjectMap
	spoolOffset      int
	spooled          []os.FileInfo
	continuation     *string
	noMore           bool
}

func aclToMode(owner *aws_s3.Owner, grants []*aws_s3.Grant) os.FileMode {
	var v os.FileMode
	for _, g := range grants {
		if g.Grantee != nil {
			if g.Grantee.ID != nil && *g.Grantee.ID == *owner.ID {
				switch *g.Permission {
				case "READ":
					v |= 0400
				case "WRITE":
					v |= 0200
				case "FULL_CONTROL":
					v |= 0600
				}
			} else if g.Grantee.URI != nil {
				switch *g.Grantee.URI {
				case "http://acs.amazonaws.com/groups/global/AuthenticatedUsers":
					switch *g.Permission {
					case "READ":
						v |= 0440
					case "WRITE":
						v |= 0220
					case "FULL_CONTROL":
						v |= 0660
					}
				case "http://acs.amazonaws.com/groups/global/AllUsers":
					switch *g.Permission {
					case "READ":
						v |= 0444
					case "WRITE":
						v |= 0222
					case "FULL_CONTROL":
						v |= 0666
					}
				}
			}
		}
	}
	return v
}

func (sol *S3ObjectLister) ListAt(result []os.FileInfo, o int64) (int, error) {
	lSuccess := prometheus.Labels{"method": "Ls", "status": "success"}
	lFailure := prometheus.Labels{"method": "Ls", "status": "failure"}
	_o, err := castInt64ToInt(o)
	if err != nil {
		mOperationStatus.With(lFailure).Inc()
		return 0, err
	}

	if _o < sol.spoolOffset {
		mOperationStatus.With(lFailure).Inc()
		return 0, fmt.Errorf("supplied position is out of range")
	}

	s := _o - sol.spoolOffset
	i := 0
	if s < len(sol.spooled) {
		n := len(result)
		if n > len(sol.spooled)-s {
			n = len(sol.spooled) - s
		}
		copy(result[i:i+n], sol.spooled[s:s+n])
		i += n
		s = len(sol.spooled)
	}

	if i >= len(result) {
		return i, nil
	}

	if sol.noMore {
		if i == 0 {
			mOperationStatus.With(lSuccess).Inc()
			return 0, io.EOF
		} else {
			return i, nil
		}
	}

	if s <= len(sol.spooled) && s >= sol.Lookback {
		sol.spooled = sol.spooled[s-sol.Lookback:]
		sol.spoolOffset += s - sol.Lookback
		s = sol.Lookback
	}

	if sol.continuation == nil {
		sol.spooled = append(sol.spooled, &ObjectFileInfo{
			_Name:         ".",
			_LastModified: time.Unix(1, 0),
			_Size:         0,
			_Mode:         0755 | os.ModeDir,
		})
		sol.spooled = append(sol.spooled, &ObjectFileInfo{
			_Name:         "..",
			_LastModified: time.Unix(1, 0),
			_Size:         0,
			_Mode:         0755 | os.ModeDir,
		})

		phObjs := sol.PhantomObjectMap.List(sol.Prefix)
		for _, phInfo := range phObjs {
			_phInfo := phInfo.GetOne()
			sol.spooled = append(sol.spooled, &ObjectFileInfo{
				_Name:         _phInfo.Key.Base(),
				_LastModified: _phInfo.LastModified,
				_Size:         _phInfo.Size,
				_Mode:         0600, // TODO
			})
		}
	}

	prefix := sol.Prefix.String()
	if prefix != "" {
		prefix += "/"
	}
	F(sol.Debug, "ListObjectsV2WithContext(Bucket=%s, Prefix=%s, Continuation=%v)", sol.Bucket, prefix, sol.continuation)
	out, err := sol.S3.ListObjectsV2WithContext(
		sol.Ctx,
		&aws_s3.ListObjectsV2Input{
			Bucket:            &sol.Bucket,
			Prefix:            &prefix,
			MaxKeys:           aws.Int64(10000),
			Delimiter:         aws.String("/"),
			ContinuationToken: sol.continuation,
		},
	)
	if err != nil {
		sol.Debug("=> ", err)
		mOperationStatus.With(lFailure).Inc()
		return i, err
	}
	F(sol.Debug, "=> { CommonPrefixes=len(%d), Contents=len(%d) }", len(out.CommonPrefixes), len(out.Contents))

	if sol.continuation == nil {
		for _, cPfx := range out.CommonPrefixes {
			sol.spooled = append(sol.spooled, &ObjectFileInfo{
				_Name:         path.Base(*cPfx.Prefix),
				_LastModified: time.Unix(1, 0),
				_Size:         0,
				_Mode:         0755 | os.ModeDir,
			})
		}
	}
	for _, obj := range out.Contents {
		// if *obj.Key == sol.Prefix {
		// 	continue
		// }
		sol.spooled = append(sol.spooled, &ObjectFileInfo{
			_Name:         path.Base(*obj.Key),
			_LastModified: *obj.LastModified,
			_Size:         *obj.Size,
			_Mode:         0644,
		})
	}
	sol.continuation = out.NextContinuationToken
	if out.NextContinuationToken == nil {
		sol.noMore = true
	}

	var n int
	if len(sol.spooled)-s > len(result)-i {
		n = len(result) - i
	} else {
		n = len(sol.spooled) - s
		if sol.noMore {
			err = io.EOF
		}
	}

	copy(result[i:i+n], sol.spooled[s:s+n])
	return i + n, err
}

type S3ObjectStat struct {
	DebugLogger
	Ctx              context.Context
	Bucket           string
	Key              Path
	Root             bool
	S3               *aws_s3.S3
	PhantomObjectMap *PhantomObjectMap
}

func (sos *S3ObjectStat) ListAt(result []os.FileInfo, o int64) (int, error) {
	F(sos.Debug, "S3ObjectStat.ListAt: len(result)=%d offset=%d", len(result), o)
	lFailure := prometheus.Labels{"method": "Stat", "status": "failure"}
	lNoObject := prometheus.Labels{"method": "Stat", "status": "noSuchObject"}
	_o, err := castInt64ToInt(o)
	if err != nil {
		mOperationStatus.With(lFailure).Inc()
		return 0, err
	}

	if len(result) == 0 {
		mOperationStatus.With(lFailure).Inc()
		return 0, nil
	}

	if _o > 0 {
		mOperationStatus.With(lFailure).Inc()
		return 0, fmt.Errorf("supplied position is out of range")
	}

	if sos.Key.IsRoot() {
		result[0] = &ObjectFileInfo{
			_Name:         "/",
			_LastModified: time.Time{},
			_Size:         0,
			_Mode:         0755 | os.ModeDir,
		}
	} else {
		phInfo := sos.PhantomObjectMap.Get(sos.Key)
		if phInfo != nil {
			_phInfo := phInfo.GetOne()
			result[0] = &ObjectFileInfo{
				_Name:         _phInfo.Key.Base(),
				_LastModified: _phInfo.LastModified,
				_Size:         _phInfo.Size,
				_Mode:         0600, // TODO
			}
		} else {
			key := sos.Key.String()
			F(sos.Debug, "GetObjectAclWithContext(Bucket=%s, Key=%s)", sos.Bucket, key)
			out, err := sos.S3.GetObjectAclWithContext(
				sos.Ctx,
				&aws_s3.GetObjectAclInput{
					Bucket: &sos.Bucket,
					Key:    &key,
				},
			)
			if err == nil {
				F(sos.Debug, "=> %v", out)
				F(sos.Debug, "HeadObjectWithContext(Bucket=%s, Key=%s)", sos.Bucket, key)
				headOut, err := sos.S3.HeadObjectWithContext(
					sos.Ctx,
					&aws_s3.HeadObjectInput{
						Bucket: &sos.Bucket,
						Key:    &key,
					},
				)
				objInfo := ObjectFileInfo{
					_Name: sos.Key.Base(),
					_Mode: aclToMode(out.Owner, out.Grants),
				}
				if err == nil {
					F(sos.Debug, "=> { ContentLength=%d, LastModified=%v }", *headOut.ContentLength, *headOut.LastModified)
					objInfo._Size = *headOut.ContentLength
					objInfo._LastModified = *headOut.LastModified
				} else {
					sos.Debug("=> ", err)
				}
				result[0] = &objInfo
			} else {
				sos.Debug("=> ", err)
				F(sos.Debug, "ListObjectsV2WithContext(Bucket=%s, Prefix=%s)", sos.Bucket, key)
				out, err := sos.S3.ListObjectsV2WithContext(
					sos.Ctx,
					&aws_s3.ListObjectsV2Input{
						Bucket:    &sos.Bucket,
						Prefix:    &key,
						MaxKeys:   aws.Int64(10000),
						Delimiter: aws.String("/"),
					},
				)
				if err != nil || (!sos.Root && len(out.CommonPrefixes) == 0) {
					sos.Debug("=> ", err)
					mOperationStatus.With(lNoObject).Inc()
					return 0, os.ErrNotExist
				}
				F(sos.Debug, "=> { CommonPrefixes=len(%d), Contents=len(%d) }", len(out.CommonPrefixes), len(out.Contents))
				result[0] = &ObjectFileInfo{
					_Name:         sos.Key.Base(),
					_LastModified: time.Time{},
					_Size:         0,
					_Mode:         0755 | os.ModeDir,
				}
			}
		}
	}
	return 1, nil
}

type S3BucketIO struct {
	Ctx                      context.Context
	Bucket                   *S3Bucket
	ReaderLookbackBufferSize int
	ReaderMinChunkSize       int
	ListerLookbackBufferSize int
	PhantomObjectMap         *PhantomObjectMap
	Perms                    Perms
	ServerSideEncryption     *ServerSideEncryptionConfig
	Now                      func() time.Time
	Log                      interface {
		ErrorLogger
		DebugLogger
	}
}

func buildKey(s3b *S3Bucket, path string) Path {
	return s3b.KeyPrefix.Join(SplitIntoPath(path))
}

func buildPath(s3b *S3Bucket, key string) (string, bool) {
	_key := SplitIntoPath(key)
	if !_key.IsPrefixed(s3b.KeyPrefix) {
		return "", false
	}
	return "/" + _key[len(s3b.KeyPrefix):].String(), true
}

func (s3io *S3BucketIO) Fileread(req *sftp.Request) (io.ReaderAt, error) {
	lSuccess := prometheus.Labels{"method": req.Method, "status": "success"}
	lFailure := prometheus.Labels{"method": req.Method, "status": "failure"}
	if !s3io.Perms.Readable {
		mOperationStatus.With(lFailure).Inc()
		return nil, fmt.Errorf("read operation not allowed as per configuration")
	}
	sess, err := aws_session.NewSession()
	if err != nil {
		mOperationStatus.With(lFailure).Inc()
		mAWSSessionError.Inc()
		return nil, err
	}
	s3 := s3io.Bucket.S3(sess)
	key := buildKey(s3io.Bucket, req.Filepath)

	phInfo := s3io.PhantomObjectMap.Get(key)
	if phInfo != nil {
		return bytes.NewReader(phInfo.Opaque.(*S3PutObjectWriter).writer.Bytes()), nil
	}

	keyStr := key.String()
	ctx := combineContext(s3io.Ctx, req.Context())
	F(s3io.Log.Debug, "GetObject(Bucket=%s, Key=%s)", s3io.Bucket.Bucket, keyStr)
	sse := s3io.ServerSideEncryption
	goo, err := s3.GetObjectWithContext(
		ctx,
		&aws_s3.GetObjectInput{
			Bucket:               &s3io.Bucket.Bucket,
			Key:                  &keyStr,
			SSECustomerAlgorithm: nilIfEmpty(sse.CustomerAlgorithm()),
			SSECustomerKey:       nilIfEmpty(sse.CustomerKey),
			SSECustomerKeyMD5:    nilIfEmpty(sse.CustomerKeyMD5),
		},
	)
	if err != nil {
		mOperationStatus.With(lFailure).Inc()
		return nil, err
	}
	oor := &S3GetObjectOutputReader{
		Ctx:          ctx,
		Goo:          goo,
		Log:          s3io.Log,
		Lookback:     s3io.ReaderLookbackBufferSize,
		MinChunkSize: s3io.ReaderMinChunkSize,
	}
	mOperationStatus.With(lSuccess).Inc()
	return oor, nil
}

func (s3io *S3BucketIO) Filewrite(req *sftp.Request) (io.WriterAt, error) {
	lSuccess := prometheus.Labels{"method": req.Method, "status": "success"}
	lFailure := prometheus.Labels{"method": req.Method, "status": "failure"}
	if !s3io.Perms.Writable {
		mOperationStatus.With(lFailure).Inc()
		return nil, fmt.Errorf("write operation not allowed as per configuration")
	}
	sess, err := aws_session.NewSession()
	if err != nil {
		mOperationStatus.With(lFailure).Inc()
		mAWSSessionError.Inc()
		return nil, err
	}
	maxObjectSize := s3io.Bucket.MaxObjectSize
	if maxObjectSize < 0 {
		maxObjectSize = int64(^uint(0) >> 1)
	}
	key := buildKey(s3io.Bucket, req.Filepath)
	info := &PhantomObjectInfo{
		Key:          key,
		Size:         0,
		LastModified: s3io.Now(),
	}
	F(s3io.Log.Debug, "S3PutObjectWriter.New(key=%s)", key)
	oow := &S3PutObjectWriter{
		Ctx:                  combineContext(s3io.Ctx, req.Context()),
		Bucket:               s3io.Bucket.Bucket,
		Key:                  key,
		S3:                   s3io.Bucket.S3(sess),
		ServerSideEncryption: s3io.ServerSideEncryption,
		Log:                  s3io.Log,
		MaxObjectSize:        maxObjectSize,
		PhantomObjectMap:     s3io.PhantomObjectMap,
		Info:                 info,
		writer:               NewBytesWriter(),
	}
	info.Opaque = oow
	s3io.PhantomObjectMap.Add(info)
	mOperationStatus.With(lSuccess).Inc()
	return oow, nil
}

func (s3io *S3BucketIO) Filecmd(req *sftp.Request) error {
	lSuccess := prometheus.Labels{"method": req.Method, "status": "success"}
	lFailure := prometheus.Labels{"method": req.Method, "status": "failure"}
	lIgnored := prometheus.Labels{"method": req.Method, "status": "ignored"}
	switch req.Method {
	case "Rename":
		if !s3io.Perms.Writable {
			mOperationStatus.With(lFailure).Inc()
			return fmt.Errorf("write operation not allowed as per configuration")
		}
		src := buildKey(s3io.Bucket, req.Filepath)
		dest := buildKey(s3io.Bucket, req.Target)
		if s3io.PhantomObjectMap.Rename(src, dest) {
			mOperationStatus.With(lIgnored).Inc()
			return nil
		}
		sess, err := aws_session.NewSession()
		if err != nil {
			mOperationStatus.With(lFailure).Inc()
			mAWSSessionError.Inc()
			return err
		}
		srcStr := src.String()
		destStr := dest.String()
		copySource := s3io.Bucket.Bucket + "/" + srcStr
		sse := s3io.ServerSideEncryption
		F(s3io.Log.Debug, "CopyObject(Bucket=%s, Key=%s, CopySource=%s, Sse=%v)", s3io.Bucket.Bucket, destStr, copySource, sse.Type)
		_, err = s3io.Bucket.S3(sess).CopyObjectWithContext(
			combineContext(s3io.Ctx, req.Context()),
			&aws_s3.CopyObjectInput{
				ACL:                  &aclPrivate,
				Bucket:               &s3io.Bucket.Bucket,
				CopySource:           &copySource,
				Key:                  &destStr,
				ServerSideEncryption: sseTypes[sse.Type],
				SSECustomerAlgorithm: nilIfEmpty(sse.CustomerAlgorithm()),
				SSECustomerKey:       nilIfEmpty(sse.CustomerKey),
				SSECustomerKeyMD5:    nilIfEmpty(sse.CustomerKeyMD5),
				SSEKMSKeyId:          nilIfEmpty(sse.KMSKeyId),
			},
		)
		if err != nil {
			s3io.Log.Debug("=> ", err)
			mOperationStatus.With(lFailure).Inc()
			return err
		}
		F(s3io.Log.Debug, "DeleteObject(Bucket=%s, Key=%s)", s3io.Bucket.Bucket, srcStr)
		_, err = s3io.Bucket.S3(sess).DeleteObjectWithContext(
			combineContext(s3io.Ctx, req.Context()),
			&aws_s3.DeleteObjectInput{
				Bucket: &s3io.Bucket.Bucket,
				Key:    &srcStr,
			},
		)
		if err != nil {
			s3io.Log.Debug("=> ", err)
			mOperationStatus.With(lFailure).Inc()
			return err
		}
		mOperationStatus.With(lSuccess).Inc()
	case "Remove":
		if !s3io.Perms.Writable {
			mOperationStatus.With(lFailure).Inc()
			return fmt.Errorf("write operation not allowed as per configuration")
		}
		key := buildKey(s3io.Bucket, req.Filepath)
		if s3io.PhantomObjectMap.Remove(key) != nil {
			mOperationStatus.With(lIgnored).Inc()
			return nil
		}
		sess, err := aws_session.NewSession()
		if err != nil {
			mOperationStatus.With(lFailure).Inc()
			mAWSSessionError.Inc()
			return err
		}
		keyStr := key.String()
		F(s3io.Log.Debug, "DeleteObject(Bucket=%s, Key=%s)", s3io.Bucket.Bucket, key)
		_, err = s3io.Bucket.S3(sess).DeleteObjectWithContext(
			combineContext(s3io.Ctx, req.Context()),
			&aws_s3.DeleteObjectInput{
				Bucket: &s3io.Bucket.Bucket,
				Key:    &keyStr,
			},
		)
		if err != nil {
			s3io.Log.Debug("=> ", err)
			mOperationStatus.With(lFailure).Inc()
			return err
		}
		mOperationStatus.With(lSuccess).Inc()
	case "Mkdir":
		if !s3io.Perms.Writable {
			mOperationStatus.With(lFailure).Inc()
			return fmt.Errorf("write operation not allowed as per configuration")
		}
		key := buildKey(s3io.Bucket, req.Filepath)
		keyStr := fmt.Sprintf("%s/", key.String())
		sess, err := aws_session.NewSession()
		if err != nil {
			mOperationStatus.With(lFailure).Inc()
			mAWSSessionError.Inc()
			return err
		}
		F(s3io.Log.Debug, "Mkdir(Bucket=%s, Key=%s)", s3io.Bucket.Bucket, keyStr)
		_, err = s3io.Bucket.S3(sess).PutObject(
			&aws_s3.PutObjectInput{
				Bucket: &s3io.Bucket.Bucket,
				Key:    &keyStr,
			},
		)
		if err != nil {
			s3io.Log.Debug("=> ", err)
			mOperationStatus.With(lFailure).Inc()
			return err
		}
		mOperationStatus.With(lSuccess).Inc()
	case "Rmdir":
		if !s3io.Perms.Writable {
			mOperationStatus.With(lFailure).Inc()
			return fmt.Errorf("write operation not allowed as per configuration")
		}
		key := buildKey(s3io.Bucket, req.Filepath)
		keyStr := fmt.Sprintf("%s/", key.String())
		sess, err := aws_session.NewSession()
		if err != nil {
			mOperationStatus.With(lFailure).Inc()
			mAWSSessionError.Inc()
			return err
		}
		F(s3io.Log.Debug, "Rmdir(Bucket=%s, Key=%s)", s3io.Bucket.Bucket, keyStr)
		_, err = s3io.Bucket.S3(sess).DeleteObject(
			&aws_s3.DeleteObjectInput{
				Bucket: &s3io.Bucket.Bucket,
				Key:    &keyStr,
			},
		)
		if err != nil {
			s3io.Log.Debug("=> ", err)
			mOperationStatus.With(lFailure).Inc()
			return err
		}
		mOperationStatus.With(lSuccess).Inc()
	}
	return nil
}

func (s3io *S3BucketIO) Filelist(req *sftp.Request) (sftp.ListerAt, error) {
	lPermErr := prometheus.Labels{"method": req.Method}
	sess, err := aws_session.NewSession()
	if err != nil {
		mAWSSessionError.Inc()
		return nil, err
	}
	switch req.Method {
	case "Stat", "ReadLink":
		if !s3io.Perms.Readable && !s3io.Perms.Listable {
			mPermissionsError.With(lPermErr).Inc()
			return nil, fmt.Errorf("stat operation not allowed as per configuration")
		}
		key := buildKey(s3io.Bucket, req.Filepath)
		return &S3ObjectStat{
			DebugLogger:      s3io.Log,
			Ctx:              combineContext(s3io.Ctx, req.Context()),
			Bucket:           s3io.Bucket.Bucket,
			Root:             key.Equal(s3io.Bucket.KeyPrefix),
			Key:              key,
			S3:               s3io.Bucket.S3(sess),
			PhantomObjectMap: s3io.PhantomObjectMap,
		}, nil
	case "List":
		if !s3io.Perms.Listable {
			mPermissionsError.With(lPermErr).Inc()
			return nil, fmt.Errorf("listing operation not allowed as per configuration")
		}
		return &S3ObjectLister{
			DebugLogger:      s3io.Log,
			Ctx:              combineContext(s3io.Ctx, req.Context()),
			Bucket:           s3io.Bucket.Bucket,
			Prefix:           buildKey(s3io.Bucket, req.Filepath),
			S3:               s3io.Bucket.S3(sess),
			Lookback:         s3io.ListerLookbackBufferSize,
			PhantomObjectMap: s3io.PhantomObjectMap,
		}, nil
	default:
		mPermissionsError.With(lPermErr).Inc()
		return nil, fmt.Errorf("unsupported method: %s", req.Method)
	}
}
