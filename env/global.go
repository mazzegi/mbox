package env

import "sync"

var global Env
var globalOnce sync.Once

var globalWD Env
var globalWDOnce sync.Once

func Glob() Env {
	globalOnce.Do(func() {
		global = Load()
	})
	return global
}

func GlobWD() Env {
	globalWDOnce.Do(func() {
		globalWD = Load()
	})
	return globalWD
}
