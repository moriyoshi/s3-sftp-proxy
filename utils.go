package main

import "fmt"

type PrintlnLike func(...interface{})

func F(p PrintlnLike, f string, args ...interface{}) {
	p(fmt.Sprintf(f, args...))
}
