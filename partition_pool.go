package main

import (
	"sync/atomic"
)

// PartitionPool pool of partitions uploaded to S3 using multi-part-upload
type PartitionPool struct {
	PartSize int
	Used     int32
	ch       chan []byte
}

// NewPartitionPool creates a new partition pool giving its size
func NewPartitionPool(partSize int, poolSize int) *PartitionPool {
	p := &PartitionPool{
		PartSize: partSize,
		ch:       make(chan []byte, poolSize),
	}
	for ; poolSize > 0; poolSize-- {
		p.ch <- make([]byte, p.PartSize)
	}
	return p
}

// Get gets a buffer from the pool
func (p *PartitionPool) Get() []byte {
	res, ok := <-p.ch
	if !ok {
		return nil
	}
	atomic.AddInt32(&p.Used, 1)
	return res
}

// Put returns a buffer into the pool
func (p *PartitionPool) Put(buf []byte) {
	p.ch <- buf
	atomic.AddInt32(&p.Used, -1)
}

// Close closes all partitions present on pool
func (p *PartitionPool) Close() {
	close(p.ch)
	_, ok := <-p.ch
	for !ok {
		_, ok = <-p.ch
	}
	p.Used = 0
}
