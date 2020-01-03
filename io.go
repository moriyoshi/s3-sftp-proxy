package main

import (
	"fmt"
	"io"
	"unsafe"
)

func castInt64ToInt(n int64) (int, error) {
	if unsafe.Sizeof(n) == unsafe.Sizeof(int(0)) {
		return int(n), nil
	}
	_n := int(n)
	if int64(_n) < n {
		return -1, fmt.Errorf("integer overflow detected when converting %#v to int", n)
	}
	return _n, nil
}

// IsEOF returns true if the error passed as parameter is related to EOF
func IsEOF(e error) bool {
	return e == io.EOF || e == io.ErrUnexpectedEOF
}

// IsTimeout returns true if the error passed as parameter is a timeout
func IsTimeout(e error) bool {
	t, ok := e.(interface{ Timeout() bool })
	if ok {
		return t.Timeout()
	}
	return false
}
