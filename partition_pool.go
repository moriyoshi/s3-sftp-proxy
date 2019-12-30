package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// PartitionPool pool of partitions uploaded to S3 using multi-part-upload
type PartitionPool struct {
	PartSize int
	Used     int32
	ch       chan []byte
	ctx      context.Context
	timeout  time.Duration
}

// NewPartitionPool creates a new partition pool giving its size
func NewPartitionPool(ctx context.Context, partSize int, poolSize int, timeout time.Duration) *PartitionPool {
	p := &PartitionPool{
		PartSize: partSize,
		ch:       make(chan []byte, poolSize),
		ctx:      ctx,
		timeout:  timeout,
	}
	mMemoryPoolsMax.Add(float64(poolSize))
	for ; poolSize > 0; poolSize-- {
		p.ch <- make([]byte, p.PartSize)
	}
	return p
}

// Get gets a buffer from the pool
func (p *PartitionPool) Get() ([]byte, error) {
	select {
	case <-p.ctx.Done():
		return nil, fmt.Errorf("partition pool get canceled")
	case <-time.After(p.timeout):
		return nil, fmt.Errorf("timeout getting partition from pool")
	case res := <-p.ch:
		mMemoryPoolsUsed.Inc()
		atomic.AddInt32(&p.Used, 1)
		return res, nil
	}
}

// Put returns a buffer into the pool
func (p *PartitionPool) Put(buf []byte) {
	p.ch <- buf
	atomic.AddInt32(&p.Used, -1)
	mMemoryPoolsUsed.Dec()
}
