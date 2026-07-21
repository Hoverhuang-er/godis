//go:build windows

package main

import "os"

func notifyWinch(ch chan<- os.Signal) {
	// SIGWINCH not available on Windows; terminal resize detection
	// is handled by periodically polling GetConsoleScreenBufferInfo.
	_ = ch
}
