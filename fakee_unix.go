// +build !windows

package main

import (
	"syscall"
)

func BuildFakeFileInfoSys() interface{} {
	return &syscall.Stat_t{Uid: 65534, Gid: 65534}
}
