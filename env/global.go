package env

import "sync"

var global Env
var globalOnce sync.Once

func Glob() Env {
	globalOnce.Do(func() {
		global = Load()
	})
	return global
}
