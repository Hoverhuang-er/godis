//go:build greenteagc

//go:build greenteagc

package greenteagc

import (
	"runtime"
	"runtime/debug"
)

func init() {
	debug.SetGCPercent(40)
	runtime.GOMAXPROCS(runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
		}()
	}
}
