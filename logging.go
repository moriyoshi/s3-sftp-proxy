package main

type DebugLogger interface {
	Debug(args ...interface{})
}

type InfoLogger interface {
	Info(args ...interface{})
}

type WarnLogger interface {
	Warn(args ...interface{})
}

type ErrorLogger interface {
	Error(args ...interface{})
}
