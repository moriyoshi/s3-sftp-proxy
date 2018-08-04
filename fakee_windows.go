// +build windows

package main

import "syscall"

func BuildFakeFileInfoSys() interface{} {
	return syscall.Win32FileAttributeData{}
}
