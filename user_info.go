package main

import (
	"fmt"
	"net"
)

type UserInfo struct {
	Addr net.Addr
	User string
}

func (uInfo UserInfo) String() string {
	return fmt.Sprintf("%s from %s", uInfo.User, uInfo.Addr.String())
}
