package main

import "testing"

func TestParseJoinFlag(t *testing.T) {
	if got := parseJoinFlag([]string{"--cli"}); got != "" {
		t.Errorf("no --join, got %q", got)
	}
	if got := parseJoinFlag([]string{"--join", "127.0.0.1:6379"}); got != "127.0.0.1:6379" {
		t.Errorf("space form failed: %q", got)
	}
	if got := parseJoinFlag([]string{"--join=http://127.0.0.1:63790"}); got != "http://127.0.0.1:63790" {
		t.Errorf("equals form failed: %q", got)
	}
}
