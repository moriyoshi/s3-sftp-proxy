// +build windows

package main

import "syscall"

// BuildFakeFileInfoSys creates a fake file information
func BuildFakeFileInfoSys() interface{} {
	return syscall.Win32FileAttributeData{}
}
