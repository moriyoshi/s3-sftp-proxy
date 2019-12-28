package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPartitionPoolGetBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := NewPartitionPool(ctx, 10, 1, 5*time.Second)
	buf, err := p.Get()
	assert.NotNil(t, buf)
	assert.NoError(t, err)
}

func TestPartitionPoolGetTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := NewPartitionPool(ctx, 10, 0, 100*time.Millisecond)
	buf, err := p.Get()
	assert.Nil(t, buf)
	assert.Error(t, err)
	assert.Regexp(t, ".*timeout.*", err.Error())
}

func TestPartitionPoolGetCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := NewPartitionPool(ctx, 10, 0, 10*time.Second)
	go cancel()
	buf, err := p.Get()
	assert.Nil(t, buf)
	assert.Error(t, err)
	assert.Regexp(t, ".*canceled.*", err.Error())
}
