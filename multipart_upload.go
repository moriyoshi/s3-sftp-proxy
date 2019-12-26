package main

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/prometheus/client_golang/prometheus"
)

// S3PartUploadState state in which a part upload is
type S3PartUploadState int

const (
	// S3PartUploadStateInitial Initial state
	S3PartUploadStateInitial = iota
	// S3PartUploadStateAdding adding data to content
	S3PartUploadStateAdding
	// S3PartUploadStateFull all data present, ready to be sent
	S3PartUploadStateFull
	// S3PartUploadStateSent already sent to S3
	S3PartUploadStateSent
	// S3PartUploadErrorSending error sending part to S3
	S3PartUploadErrorSending
)

// S3PartToUpload S3 part to be uploaded
type S3PartToUpload struct {
	// Part content
	content []byte
	// Part number (starting from 1)
	partNumber int64
	// Pending bytes to fill this part
	pb int
	// S3MultipartUploadWriter that contains this part
	uw *S3MultipartUploadWriter
	// Mutex to avoid problems accessing to the same part upload
	mtx sync.Mutex
	// State to know how to treat this part
	state S3PartUploadState
}

func (pto *S3PartToUpload) getContent() []byte {
	return pto.content[0 : len(pto.content)-pto.pb]
}

// S3MultipartUploadWriter uploads multiple parts to S3 having a writer interface
type S3MultipartUploadWriter struct {
	Ctx                  context.Context
	Bucket               string
	Key                  Path
	S3                   s3iface.S3API
	ServerSideEncryption *ServerSideEncryptionConfig
	Log                  interface {
		DebugLogger
		WarnLogger
		ErrorLogger
	}
	MaxObjectSize     int64
	PartitionPool     *PartitionPool
	Info              *PhantomObjectInfo
	PhantomObjectMap  *PhantomObjectMap
	RequestMethod     string
	mtx               sync.Mutex
	completedParts    []*aws_s3.CompletedPart
	parts             []*S3PartToUpload
	multiPartUploadID *string
	err               error
	uploadGroup       sync.WaitGroup
	UploadChan        chan<- *S3PartToUpload
}

// Close closes multipart upload writer
func (u *S3MultipartUploadWriter) Close() error {
	F(u.Log.Debug, "S3MultipartUploadWriter.Close")

	var err error

	u.PhantomObjectMap.RemoveByInfoPtr(u.Info)

	u.mtx.Lock()
	defer u.mtx.Unlock()

	// Only one part -> use PutObject
	if u.multiPartUploadID == nil {
		err = u.s3PutObject(u.parts[0].getContent())
	} else {
		// More than 1 part -> MultiPartUpload used before, we have to send latest part, wait until all parts will be uploaded and complete the job
		err = u.err
		if err == nil {
			u.mtx.Unlock()

			u.enqueueUpload(u.parts[len(u.parts)-1])
			u.uploadGroup.Wait()

			u.mtx.Lock()

			err = u.err
			if err == nil {
				err = u.s3CompleteMultipartUpload()
			} else {
				u.s3AbortMultipartUpload()
			}
		}
	}

	if err != nil {
		mOperationStatus.With(prometheus.Labels{"method": u.RequestMethod, "status": "failure"}).Inc()
	} else {
		mOperationStatus.With(prometheus.Labels{"method": u.RequestMethod, "status": "success"}).Inc()
	}
	return err
}

// WriteAt stores on memory the data sent to be uploaded and uploads it when a part
// is completed
func (u *S3MultipartUploadWriter) WriteAt(buf []byte, off int64) (int, error) {
	pending := int64(len(buf))
	offFinal := off + pending
	partSize := int64(u.PartitionPool.PartSize)
	partNumberInitial := int(off / partSize)
	partOffsetInitial := off % partSize

	var err error
	u.mtx.Lock()
	err = u.err
	if err == nil && u.MaxObjectSize >= 0 && offFinal > u.MaxObjectSize {
		err = fmt.Errorf("file too large: maximum allowed size is %d bytes", u.MaxObjectSize)
	}

	if err == nil {
		partNumberFinal := int((off + pending - 1) / partSize)

		F(u.Log.Debug, "len(buf)=%d, off=%d, partNumberInitial=%d, partOffsetInitial=%d", len(buf), off, partNumberInitial, partOffsetInitial)
		u.Info.SetSizeIfGreater(offFinal)
		if len(u.parts) <= partNumberFinal {
			newParts := make([]*S3PartToUpload, partNumberFinal+1)
			copy(newParts, u.parts)
			u.parts = newParts
		}
		if u.multiPartUploadID == nil && (partNumberFinal > 0 || len(buf) == u.PartitionPool.PartSize) {
			err = u.s3CreateMultipartUpload()
		}
	}

	if err != nil {
		u.s3AbortMultipartUpload()
		mOperationStatus.With(prometheus.Labels{"method": u.RequestMethod, "status": "failure"}).Inc()
		u.mtx.Unlock()
		return 0, err
	}
	u.mtx.Unlock()

	partNumber := partNumberInitial
	partOffset := partOffsetInitial
	for pending > 0 {
		u.mtx.Lock()
		part := u.parts[partNumber]
		if part == nil {
			part = &S3PartToUpload{
				content:    u.PartitionPool.Get().([]byte),
				pb:         u.PartitionPool.PartSize,
				uw:         u,
				state:      S3PartUploadStateAdding,
				partNumber: int64(partNumber + 1),
			}
			u.parts[partNumber] = part
		}
		u.mtx.Unlock()
		part.mtx.Lock()
		if part.state == S3PartUploadStateFull || part.state == S3PartUploadStateSent {
			part.mtx.Unlock()
			F(u.Log.Warn, "Part %d full and received data at offset %d. Ignoring.", partNumber, off)
			return len(buf), nil
		}

		partOffsetFinal := partOffset + pending
		if partOffsetFinal > partSize {
			partOffsetFinal = partSize
		}
		partCopied := partOffsetFinal - partOffset
		copy(part.content[partOffset:partOffsetFinal], buf[off:off+partCopied])
		part.pb -= int(partCopied)
		if part.pb == 0 {
			u.enqueueUpload(part)
		}
		part.mtx.Unlock()
		partNumber++
		pending -= partCopied
		off += partCopied
		partOffset = 0
	}
	return len(buf), nil
}

func (u *S3MultipartUploadWriter) enqueueUpload(part *S3PartToUpload) {
	if part.state <= S3PartUploadStateFull {
		part.state = S3PartUploadStateFull
		u.uploadGroup.Add(1)
		u.UploadChan <- part
	}
}

// S3 related actions
func (u *S3MultipartUploadWriter) s3CreateMultipartUpload() error {
	key := u.Info.GetOne().Key.String()
	sse := u.ServerSideEncryption
	F(u.Log.Debug, "CreateMultipartUpload(Bucket=%s, Key=%s, Sse=%v)", u.Bucket, key, sse)

	params := &aws_s3.CreateMultipartUploadInput{
		ACL:                  &aclPrivate,
		Bucket:               &u.Bucket,
		Key:                  &key,
		ServerSideEncryption: sseTypes[sse.Type],
		SSECustomerAlgorithm: nilIfEmpty(sse.CustomerAlgorithm()),
		SSECustomerKey:       nilIfEmpty(sse.CustomerKey),
		SSECustomerKeyMD5:    nilIfEmpty(sse.CustomerKeyMD5),
		SSEKMSKeyId:          nilIfEmpty(sse.KMSKeyId),
	}

	resp, err := u.S3.CreateMultipartUploadWithContext(u.Ctx, params)
	if err != nil {
		u.Log.Debug("=> ", err)
		F(u.Log.Error, "failed to create multipart upload: %s", err.Error())
		return err
	}
	u.Log.Debug("=> OK")
	u.multiPartUploadID = resp.UploadId
	return nil
}

func (u *S3MultipartUploadWriter) s3PutObject(content []byte) error {
	key := u.Info.GetOne().Key.String()
	sse := u.ServerSideEncryption
	F(u.Log.Debug, "PutObject(Bucket=%s, Key=%s, Sse=%v)", u.Bucket, key, sse)

	params := &aws_s3.PutObjectInput{
		ACL:                  &aclPrivate,
		Body:                 bytes.NewReader(content),
		Bucket:               &u.Bucket,
		Key:                  &key,
		ServerSideEncryption: sseTypes[sse.Type],
		SSECustomerAlgorithm: nilIfEmpty(sse.CustomerAlgorithm()),
		SSECustomerKey:       nilIfEmpty(sse.CustomerKey),
		SSECustomerKeyMD5:    nilIfEmpty(sse.CustomerKeyMD5),
		SSEKMSKeyId:          nilIfEmpty(sse.KMSKeyId),
	}
	if _, err := u.S3.PutObjectWithContext(u.Ctx, params); err != nil {
		u.Log.Debug("=> ", err)
		F(u.Log.Error, "failed to put object: %s", err.Error())
		return err
	}
	u.Log.Debug("=> OK")

	return nil
}

func (u *S3MultipartUploadWriter) s3AbortMultipartUpload() error {
	if u.multiPartUploadID != nil {
		key := u.Info.GetOne().Key.String()
		sse := u.ServerSideEncryption
		F(u.Log.Debug, "AbortMultipartUpload(Bucket=%s, Key=%s, Sse=%v)", u.Bucket, key, sse)

		params := &aws_s3.AbortMultipartUploadInput{
			Bucket:   &u.Bucket,
			Key:      &key,
			UploadId: u.multiPartUploadID,
		}
		u.multiPartUploadID = nil
		if _, err := u.S3.AbortMultipartUploadWithContext(u.Ctx, params); err != nil {
			u.Log.Debug("=> ", err)
			F(u.Log.Error, "failed to abort multipart upload: %s", err.Error())
			return err
		}
		u.Log.Debug("=> OK")
	}

	return nil
}

func (u *S3MultipartUploadWriter) s3CompleteMultipartUpload() error {
	key := u.Info.GetOne().Key.String()
	sse := u.ServerSideEncryption
	F(u.Log.Debug, "CompleteMultipartUpload(Bucket=%s, Key=%s, Sse=%v)", u.Bucket, key, sse)

	params := &aws_s3.CompleteMultipartUploadInput{
		Bucket:          &u.Bucket,
		Key:             &key,
		UploadId:        u.multiPartUploadID,
		MultipartUpload: &aws_s3.CompletedMultipartUpload{Parts: u.completedParts},
	}
	if _, err := u.S3.CompleteMultipartUploadWithContext(u.Ctx, params); err != nil {
		u.Log.Debug("=> ", err)
		F(u.Log.Error, "failed to complete multipart upload: %s", err.Error())
		return err
	}
	u.Log.Debug("=> OK")
	return nil
}

func (u *S3MultipartUploadWriter) s3UploadPart(part *S3PartToUpload) error {
	key := u.Info.GetOne().Key.String()
	sse := u.ServerSideEncryption
	F(u.Log.Debug, "UploadPart(Bucket=%s, Key=%s, Sse=%v, part=%d)", u.Bucket, key, sse, part.partNumber)

	params := &aws_s3.UploadPartInput{
		Bucket:               &u.Bucket,
		Key:                  &key,
		Body:                 bytes.NewReader(part.getContent()),
		UploadId:             u.multiPartUploadID,
		SSECustomerAlgorithm: nilIfEmpty(sse.CustomerAlgorithm()),
		SSECustomerKey:       nilIfEmpty(sse.CustomerKey),
		SSECustomerKeyMD5:    nilIfEmpty(sse.CustomerKeyMD5),
		PartNumber:           &part.partNumber,
	}

	resp, err := u.S3.UploadPartWithContext(u.Ctx, params)

	if err != nil {
		u.Log.Debug("=> ", err)
		F(u.Log.Error, "failed to upload part: %s", err.Error())
		return err
	}

	if len(u.completedParts) < len(u.parts) {
		newCompletedParts := make([]*aws_s3.CompletedPart, len(u.parts))
		copy(newCompletedParts, u.completedParts)
		u.completedParts = newCompletedParts
	}
	completed := &aws_s3.CompletedPart{ETag: resp.ETag, PartNumber: &(part.partNumber)}
	u.completedParts[part.partNumber-1] = completed
	return nil
}

// geterr is a thread-safe getter for the error object
func (u *S3MultipartUploadWriter) geterr() error {
	u.mtx.Lock()
	defer u.mtx.Unlock()

	return u.err
}

// seterr is a thread-safe setter for the error object
func (u *S3MultipartUploadWriter) seterr(e error) {
	u.mtx.Lock()
	defer u.mtx.Unlock()

	u.err = e
}

// S3UploadWorkers object to manage S3 upload workers
type S3UploadWorkers struct {
	ctx     context.Context
	workers int
	log     DebugLogger
	wg      sync.WaitGroup
}

// NewS3UploadWorkers creates new upload workers to take pending part uploads from a channel and
// upload them to S3
func NewS3UploadWorkers(ctx context.Context, workers int, log DebugLogger) *S3UploadWorkers {
	return &S3UploadWorkers{
		ctx:     ctx,
		workers: workers,
		log:     log,
	}
}

// Start starts workers
func (w *S3UploadWorkers) Start() chan<- *S3PartToUpload {
	uploadChan := make(chan *S3PartToUpload)

	w.wg.Add(w.workers)
	for c := 0; c < w.workers; c++ {
		go func(wn int) {
			defer w.wg.Done()

			for {
				select {
				case <-w.ctx.Done():
					F(w.log.Debug, "S3 upload worker %d ended", wn)
					return
				case part, ok := <-uploadChan:
					if !ok {
						F(w.log.Debug, "S3 upload worker %d finished because channel is closed", wn)
						return
					}
					w.uploadPart(part)
				}
			}
		}(c)
	}

	return uploadChan
}

// WaitForCompletion waits until all workers finish their job
func (w *S3UploadWorkers) WaitForCompletion() {
	w.wg.Wait()
}

func (w *S3UploadWorkers) uploadPart(part *S3PartToUpload) {
	part.mtx.Lock()
	defer part.mtx.Unlock()

	u := part.uw
	defer u.uploadGroup.Done()

	if part.state != S3PartUploadStateFull {
		F(w.log.Debug, "Part to upload state invalid.")
		return
	}

	err := u.s3UploadPart(part)
	u.PartitionPool.Put(part.content)

	if err != nil {
		part.state = S3PartUploadErrorSending
		u.seterr(err)
	} else {
		part.state = S3PartUploadStateSent
	}
}
