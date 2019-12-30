package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// MemoryBufferPool pool of memory buffers
// Used to reduce the GC generated when a memory buffer of the same size is needed
type MemoryBufferPool struct {
	BufSize int
	Used    int32
	ch      chan []byte
	ctx     context.Context
	timeout time.Duration
}

// NewMemoryBufferPool creates a new partition pool giving its size
func NewMemoryBufferPool(ctx context.Context, bufSize int, poolSize int, timeout time.Duration) *MemoryBufferPool {
	mbp := &MemoryBufferPool{
		BufSize: bufSize,
		ch:      make(chan []byte, poolSize),
		ctx:     ctx,
		timeout: timeout,
	}
	mMemoryBufferPoolMax.Add(float64(poolSize))
	for ; poolSize > 0; poolSize-- {
		mbp.ch <- make([]byte, mbp.BufSize)
	}
	return mbp
}

// Get gets a buffer from the pool
func (mbp *MemoryBufferPool) Get() ([]byte, error) {
	select {
	case <-mbp.ctx.Done():
		return nil, fmt.Errorf("partition pool get canceled")
	case <-time.After(mbp.timeout):
		mMemoryBufferPoolTimeouts.Inc()
		return nil, fmt.Errorf("timeout getting partition from pool")
	case res := <-mbp.ch:
		mMemoryBufferPoolUsed.Inc()
		atomic.AddInt32(&mbp.Used, 1)
		return res, nil
	}
}

// Put returns a buffer into the pool
func (mbp *MemoryBufferPool) Put(buf []byte) {
	mbp.ch <- buf
	atomic.AddInt32(&mbp.Used, -1)
	mMemoryBufferPoolUsed.Dec()
}
