//go:build windows

package monitoring

// cpuSeconds returns the process system and user CPU time in seconds.
// Not implemented on Windows; report zero so the dashboard still renders.
func cpuSeconds() (sysSec, userSec float64) {
	return 0, 0
}

// diskUsage returns used and total capacity in bytes for the filesystem
// containing dir. Not implemented on Windows; report zero.
func diskUsage(dir string) (used, capacity uint64) {
	return 0, 0
}
