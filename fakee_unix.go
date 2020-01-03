// +build !windows

package main

import (
	"syscall"
)

// BuildFakeFileInfoSys creates a fake file information
func BuildFakeFileInfoSys() interface{} {
	return &syscall.Stat_t{Uid: 65534, Gid: 65534}
}
