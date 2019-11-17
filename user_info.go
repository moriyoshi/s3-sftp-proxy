package main

import "net"

type UserInfo struct {
	Addr  net.Addr
	User string
}
