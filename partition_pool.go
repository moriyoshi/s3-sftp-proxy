package main

import (
	"sync"
)

// PartitionPool pool of partitions uploaded to S3 using multi-part-upload
type PartitionPool struct {
	sync.Pool
	PartSize int
}

// NewPartitionPool creates a new partition pool giving its size
func NewPartitionPool(partSize int) *PartitionPool {
	p := &PartitionPool{PartSize: partSize}

	p.New = func() interface{} {
		return make([]byte, p.PartSize)
	}

	return p
}
