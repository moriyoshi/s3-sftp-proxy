package main

import (
	"fmt"
	"io"
	"unsafe"
)

type BytesWriter struct {
	buf []byte
	pos int
}

func NewBytesWriter() *BytesWriter {
	return &BytesWriter{
		buf: []byte{},
	}
}

func castInt64ToInt(n int64) (int, error) {
	if unsafe.Sizeof(n) == unsafe.Sizeof(int(0)) {
		return int(n), nil
	} else {
		_n := int(n)
		if int64(_n) < n {
			return -1, fmt.Errorf("integer overflow detected when converting %#v to int", n)
		}
		return _n, nil
	}

}

func (bw *BytesWriter) Close() error {
	return nil
}

// Resize the buffer capacity so the new size is at least the value of newCap.
func (bw *BytesWriter) grow(newCap int) {
	if cap(bw.buf) >= newCap {
		return
	}
	i := cap(bw.buf)
	if i < 2 {
		i = 2
	}
	for i < newCap {
		i = i + i/2
		if i < cap(bw.buf) {
			panic("allocation failure")
		}
	}
	newBuf := make([]byte, len(bw.buf), i)
	copy(newBuf, bw.buf)
	bw.buf = newBuf
}

func (bw *BytesWriter) Truncate(n int64) error {
	_n, err := castInt64ToInt(n)
	if err != nil {
		return err
	}
	bw.buf = bw.buf[0:_n]
	if bw.pos > _n {
		bw.pos = _n
	}
	return nil
}

func (bw *BytesWriter) Seek(offset int64, whence int) (int64, error) {
	_o, err := castInt64ToInt(offset)
	if err != nil {
		return -1, err
	}
	var newPos int
	switch whence {
	case 0:
		newPos = _o
	case 1:
		newPos = bw.pos + _o
	case 2:
		newPos = len(bw.buf) + _o
	}
	if newPos < len(bw.buf) {
		bw.grow(newPos)
		bw.buf = bw.buf[0:newPos]
	}
	bw.pos = newPos
	return int64(newPos), nil
}

func (bw *BytesWriter) Write(p []byte) (n int, err error) {
	bw.grow(bw.pos + len(p))
	copy(bw.buf[bw.pos:bw.pos+len(p)], p)
	return len(p), nil
}

func (bw *BytesWriter) WriteAt(p []byte, offset int64) (n int, err error) {
	_o, err := castInt64ToInt(offset)
	if err != nil {
		return -1, err
	}
	req := _o + len(p)
	if req > len(bw.buf) {
		bw.grow(req)
		bw.buf = bw.buf[0:req]
	}
	copy(bw.buf[_o:req], p)
	return len(p), nil
}

func (bw *BytesWriter) Size() int64 {
	return int64(len(bw.buf))
}

func (bw *BytesWriter) Bytes() []byte {
	return bw.buf
}

func IsEOF(e error) bool {
	return e == io.EOF || e == io.ErrUnexpectedEOF
}
