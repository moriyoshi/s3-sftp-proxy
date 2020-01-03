package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	fake_log "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

type mockedS3 struct {
	s3iface.S3API

	totalBytes int

	errorUploadPartCalls              int
	errorPutObjectCalls               int
	errorCreateMultipartUploadCalls   int
	errorCompleteMultipartUploadCalls int
	errorAbortMultipartUploadCalls    int

	partSize                     int
	uploadPartCalls              int
	putObjectCalls               int
	createMultipartUploadCalls   int
	completeMultipartUploadCalls int
	abortMultipartUploadCalls    int
}

var TestUploadID = "test"

func (m *mockedS3) PutObjectWithContext(ctx aws.Context, input *aws_s3.PutObjectInput, _ ...request.Option) (*aws_s3.PutObjectOutput, error) {
	m.putObjectCalls++
	if m.errorPutObjectCalls != 0 && m.errorPutObjectCalls >= m.putObjectCalls {
		return nil, fmt.Errorf("Error on input test")
	}

	b, err := ioutil.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != byte(i%10+48) {
			return nil, fmt.Errorf("Found incorrect char at pos %d -> %c != %c", i, b[i], byte(i%10+49))
		}
	}
	m.totalBytes += len(b)
	return nil, nil
}

func (m *mockedS3) CreateMultipartUploadWithContext(_ aws.Context, _ *aws_s3.CreateMultipartUploadInput, _ ...request.Option) (*aws_s3.CreateMultipartUploadOutput, error) {
	m.createMultipartUploadCalls++
	if m.errorCreateMultipartUploadCalls != 0 && m.errorCreateMultipartUploadCalls >= m.createMultipartUploadCalls {
		return nil, fmt.Errorf("Error on input test")
	}

	return &aws_s3.CreateMultipartUploadOutput{
		UploadId: &TestUploadID,
	}, nil
}

func (m *mockedS3) UploadPartWithContext(_ aws.Context, input *aws_s3.UploadPartInput, _ ...request.Option) (*aws_s3.UploadPartOutput, error) {
	m.uploadPartCalls++
	if m.errorUploadPartCalls != 0 && m.errorUploadPartCalls >= m.uploadPartCalls {
		return nil, fmt.Errorf("Error on input test")
	}

	off := int((*input.PartNumber)-1) * m.partSize
	b, err := ioutil.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != byte((off+i)%10+48) {
			return nil, fmt.Errorf("Found incorrect char at part %d, pos %d -> %c != %c", *input.PartNumber, i, b[i], byte((off+i)%10+49))
		}
	}
	m.totalBytes += len(b)
	etag := fmt.Sprintf("etagTest%d", *input.PartNumber-1)
	return &aws_s3.UploadPartOutput{
		ETag: &etag,
	}, nil
}

func (m *mockedS3) CompleteMultipartUploadWithContext(_ aws.Context, input *aws_s3.CompleteMultipartUploadInput, _ ...request.Option) (*aws_s3.CompleteMultipartUploadOutput, error) {
	m.completeMultipartUploadCalls++
	if m.errorCompleteMultipartUploadCalls != 0 && m.errorCompleteMultipartUploadCalls >= m.completeMultipartUploadCalls {
		return nil, fmt.Errorf("Error on input test")
	}

	if *input.UploadId != TestUploadID {
		return nil, fmt.Errorf("UploadID does not match")
	}

	for i, v := range input.MultipartUpload.Parts {
		expectedEtag := fmt.Sprintf("etagTest%d", i)
		if *v.PartNumber != int64(i+1) || *v.ETag != expectedEtag {
			return nil, fmt.Errorf("etag or partnumber does not match: PartNumber(%d), ETag (%s)", *v.PartNumber, *v.ETag)
		}
	}
	return nil, nil
}

func (m *mockedS3) AbortMultipartUploadWithContext(_ aws.Context, _ *aws_s3.AbortMultipartUploadInput, _ ...request.Option) (*aws_s3.AbortMultipartUploadOutput, error) {
	m.abortMultipartUploadCalls++
	if m.errorAbortMultipartUploadCalls != 0 && m.errorAbortMultipartUploadCalls >= m.abortMultipartUploadCalls {
		return nil, fmt.Errorf("Error on input test")
	}
	return nil, nil
}

// Tests
func TestMultipartUploadSinglePart(t *testing.T) {
	partSize := 50
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize: partSize,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 1, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("01234567890"), 0)
	assert.NoError(t, err)
	assert.NoError(t, u.Close())
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 1, m.putObjectCalls)
	assert.Equal(t, 0, m.uploadPartCalls)
	assert.Equal(t, 0, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 11, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadPendingPartOnSinglePut(t *testing.T) {
	partSize := 20
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize: partSize,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 1, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("0123456789"), 7)
	assert.NoError(t, err)
	assert.Error(t, u.Close())
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 0, m.uploadPartCalls)
	assert.Equal(t, 0, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 0, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadErrorPutObject(t *testing.T) {
	partSize := 20
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize:            partSize,
		errorPutObjectCalls: 1,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 1, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("0123456789"), 0)
	assert.NoError(t, err)
	assert.Error(t, u.Close())
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 1, m.putObjectCalls)
	assert.Equal(t, 0, m.uploadPartCalls)
	assert.Equal(t, 0, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 0, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadSinglePartFullUsesMultipart(t *testing.T) {
	partSize := 10
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize: partSize,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 1, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("0123456789"), 0)
	assert.NoError(t, err)
	assert.NoError(t, u.Close())
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 1, m.uploadPartCalls)
	assert.Equal(t, 1, m.createMultipartUploadCalls)
	assert.Equal(t, 1, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 10, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadFillingPart(t *testing.T) {
	partSize := 10
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize: partSize,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 10, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("01234"), 0)
	assert.NoError(t, err)
	_, err = u.WriteAt([]byte("56789"), 5)
	assert.NoError(t, err)
	_, err = u.WriteAt([]byte("0123456789"), 10)
	assert.NoError(t, err)
	assert.NoError(t, u.Close())
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 2, m.uploadPartCalls)
	assert.Equal(t, 1, m.createMultipartUploadCalls)
	assert.Equal(t, 1, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 20, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadMultiPartNotLockings(t *testing.T) {
	partSize := 10
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize: partSize,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 10, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("0123456789"), 0)
	assert.NoError(t, err)
	_, err = u.WriteAt([]byte("0123456789"), 0)
	assert.NoError(t, err)
	assert.NoError(t, u.Close())
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 1, m.uploadPartCalls)
	assert.Equal(t, 1, m.createMultipartUploadCalls)
	assert.Equal(t, 1, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 10, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadMultiplePartSingleWriteAt(t *testing.T) {
	partSize := 15
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize: partSize,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 1, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("01234567890123456789012345"), 0)
	assert.NoError(t, err)
	assert.NoError(t, u.Close())
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 2, m.uploadPartCalls)
	assert.Equal(t, 1, m.createMultipartUploadCalls)
	assert.Equal(t, 1, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 26, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadMultiplePartMultipleWriteAt(t *testing.T) {
	partSize := 15
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize: partSize,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 1, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	for c := 0; c < 2; c++ {
		_, err := u.WriteAt([]byte("0123456789"), 10*int64(c))
		assert.NoError(t, err)
	}
	assert.NoError(t, u.Close())
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 2, m.uploadPartCalls)
	assert.Equal(t, 1, m.createMultipartUploadCalls)
	assert.Equal(t, 1, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadMultiplePartIgnoredOverlapping(t *testing.T) {
	partSize := 15
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize: partSize,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 1, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	for c := 0; c < 2; c++ {
		_, err := u.WriteAt([]byte("0123456789"), 0)
		assert.NoError(t, err)
	}
	assert.NoError(t, u.Close())
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 1, m.putObjectCalls)
	assert.Equal(t, 0, m.uploadPartCalls)
	assert.Equal(t, 0, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 10, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadMaxObjectSizeErrorSingleWrite(t *testing.T) {
	partSize := 15
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize: partSize,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 1, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          3,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("0123456789"), 0)
	assert.Error(t, err)
	assert.Equal(t, u.Close(), err)
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 0, m.uploadPartCalls)
	assert.Equal(t, 0, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 0, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadMaxObjectSizeErrorSeveralWrites(t *testing.T) {
	partSize := 7
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize: partSize,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 1, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          12,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("0123456789"), 0)
	assert.NoError(t, err)
	_, err = u.WriteAt([]byte("0123456789"), 10)
	assert.Error(t, err)
	assert.Equal(t, u.Close(), err)
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 1, m.uploadPartCalls)
	assert.Equal(t, 1, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 1, m.abortMultipartUploadCalls)
	assert.Equal(t, 7, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadPendingParts(t *testing.T) {
	partSize := 10
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize: partSize,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 5, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("0123456789"), 7)
	assert.NoError(t, err)
	assert.Error(t, u.Close())
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 1, m.uploadPartCalls)
	assert.Equal(t, 1, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 1, m.abortMultipartUploadCalls)
	assert.Equal(t, 0, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadErrorCreatingMultipartUploadOnClose(t *testing.T) {
	partSize := 10
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize:                        partSize,
		errorCreateMultipartUploadCalls: 1,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 5, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("0123456789"), 7)
	assert.NoError(t, err)
	assert.Error(t, u.Close())
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 0, m.uploadPartCalls)
	assert.Equal(t, 1, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 0, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadErrorCreatingMultipartUploadOnWrite(t *testing.T) {
	partSize := 10
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize:                        partSize,
		errorCreateMultipartUploadCalls: 1,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 5, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("0123456789012"), 0)
	assert.Error(t, err)
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 0, m.uploadPartCalls)
	assert.Equal(t, 1, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 0, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadErrorUploadingPartDetectedOnNextWrite(t *testing.T) {
	partSize := 10
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize:             partSize,
		errorUploadPartCalls: 1,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 5, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("012345678901234"), 0)
	assert.NoError(t, err)
	close(ch)
	w.WaitForCompletion()
	_, err = u.WriteAt([]byte("01"), 14)
	assert.Error(t, err)
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 1, m.uploadPartCalls)
	assert.Equal(t, 1, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 1, m.abortMultipartUploadCalls)
	assert.Equal(t, 0, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadPartPending(t *testing.T) {
	partSize := 10
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize:             partSize,
		errorUploadPartCalls: 1,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 5, 5*time.Second),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("012345678901"), 0)
	assert.NoError(t, err)
	_, err = u.WriteAt([]byte("01"), 16)
	assert.NoError(t, err)
	assert.Error(t, u.Close())
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 1, m.uploadPartCalls)
	assert.Equal(t, 1, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 1, m.abortMultipartUploadCalls)
	assert.Equal(t, 0, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadPoolFull(t *testing.T) {
	partSize := 10
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize:             partSize,
		errorUploadPartCalls: 1,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 1, 100*time.Millisecond),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("012345678901"), 7)
	assert.Error(t, err)
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 0, m.uploadPartCalls)
	assert.Equal(t, 0, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 0, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

func TestMultipartUploadErrorasdf(t *testing.T) {
	partSize := 10
	log, _ := fake_log.NewNullLogger()
	w := NewS3UploadWorkers(context.Background(), 1, log)
	ch := w.Start()
	m := &mockedS3{
		partSize:             partSize,
		errorUploadPartCalls: 1,
	}
	u := &S3MultipartUploadWriter{
		Ctx:                    context.Background(),
		S3:                     m,
		UploadMemoryBufferPool: NewMemoryBufferPool(context.Background(), partSize, 1, 100*time.Millisecond),
		RequestMethod:          "read",
		Log:                    log,
		PhantomObjectMap:       NewPhantomObjectMap(),
		Info:                   &PhantomObjectInfo{Key: Path{"", "a", "b"}},
		UploadChan:             ch,
		MaxObjectSize:          -1,
		ServerSideEncryption:   &ServerSideEncryptionConfig{},
	}
	_, err := u.WriteAt([]byte("012345678901"), 7)
	assert.Error(t, err)
	close(ch)
	w.WaitForCompletion()
	assert.Equal(t, 0, m.putObjectCalls)
	assert.Equal(t, 0, m.uploadPartCalls)
	assert.Equal(t, 0, m.createMultipartUploadCalls)
	assert.Equal(t, 0, m.completeMultipartUploadCalls)
	assert.Equal(t, 0, m.abortMultipartUploadCalls)
	assert.Equal(t, 0, m.totalBytes)
	assertPartsWithState(t, u, 0, S3PartUploadStateAdding)
}

// Helpers
func assertPartsWithState(t *testing.T, u *S3MultipartUploadWriter, expected int, state S3PartUploadState) {
	res := 0
	for _, part := range u.parts {
		if part != nil && part.state == state {
			res++
		}
	}
	assert.Equal(t, expected, res, "Found %d parts with state %d, and expected %d", res, state, expected)
}
