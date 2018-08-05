package main

import (
	"context"
	"reflect"
	"time"
)

type mergedContext struct {
	ctxs     []context.Context
	doneChan chan struct{}
	err      error
}

func (ctxs *mergedContext) Deadline() (time.Time, bool) {
	retval := time.Time{}
	retvalAvail := false
	for _, ctx := range ctxs.ctxs {
		if dl, ok := ctx.Deadline(); ok {
			if !retval.IsZero() || retval.After(dl) {
				retval = dl
				retvalAvail = true
			}
		}
	}
	return retval, retvalAvail
}

func (ctxs *mergedContext) Done() <-chan struct{} {
	return ctxs.doneChan
}

func (ctxs *mergedContext) Err() error {
	return ctxs.err
}

func (ctxs *mergedContext) Value(key interface{}) interface{} {
	for _, ctx := range ctxs.ctxs {
		v := ctx.Value(key)
		if v != nil {
			return v
		}
	}
	return nil
}

func (ctxs *mergedContext) watcher() {
	if len(ctxs.ctxs) == 2 {
		go func() {
			select {
			case <-ctxs.ctxs[0].Done():
				ctxs.err = ctxs.ctxs[0].Err()
			case <-ctxs.ctxs[1].Done():
				ctxs.err = ctxs.ctxs[0].Err()
			}
			close(ctxs.doneChan)
		}()
	} else {
		cases := []reflect.SelectCase{}
		for _, ctx := range ctxs.ctxs {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ctx.Done()),
			})
		}
		go func() {
			chosen, _, _ := reflect.Select(cases)
			ctxs.err = ctxs.ctxs[chosen].Err()
			close(ctxs.doneChan)
		}()
	}
}

func combineContext(ctxs ...context.Context) context.Context {
	if len(ctxs) == 1 {
		return ctxs[0]
	}
	ctx := &mergedContext{
		ctxs:     ctxs,
		doneChan: make(chan struct{}),
		err:      nil,
	}
	ctx.watcher()
	return ctx
}
