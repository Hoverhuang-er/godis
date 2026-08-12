//go:build !windows

package monitoring

import (
	"syscall"
)

// cpuSeconds returns the process system and user CPU time in seconds.
func cpuSeconds() (sysSec, userSec float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	sys := ru.Stime
	user := ru.Utime
	return float64(sys.Sec) + float64(sys.Usec)/1e6,
		float64(user.Sec) + float64(user.Usec)/1e6
}

// diskUsage returns used and total capacity in bytes for the filesystem
// containing dir. On error it returns zeros.
func diskUsage(dir string) (used, capacity uint64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0
	}
	capacity = st.Blocks * uint64(st.Bsize)
	free := st.Bfree * uint64(st.Bsize)
	if capacity < free {
		return 0, 0
	}
	return capacity - free, capacity
}
