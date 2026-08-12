package database

import (
	"fmt"
	"os"
	"path/filepath"

	"log/slog"

	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
)

// Repl backlog persistence (WAL-driven PSYNC).
//
// The replication backlog is normally kept in memory only: a master restart
// resets both replid and backlog, forcing every slave into a full resync.
// With WAL persistence the backlog is mirrored to a local append-only file so
// a restarted master can still answer `PSYNC replid offset` with +CONTINUE as
// long as the requested offset falls inside the persisted window
// (kvrocks-style partial resync).

const (
	walBacklogFilename = "repl-backlog.wal"
	replidFilename     = "repl-id"
	maxBacklogSize     = 10 * 1024 * 1024 // 10MB, matches in-memory cap
)

// openReplBacklog creates or reopens the persisted replication backlog.
// walDir may be empty to run without persistence (memory only).
func openReplBacklog(walDir string) (*replBacklog, error) {
	b := &replBacklog{}
	if walDir == "" {
		return b, nil
	}
	if err := os.MkdirAll(walDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("create repl wal dir failed: %w", err)
	}
	path := filepath.Join(walDir, walBacklogFilename)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open repl wal failed: %w", err)
	}
	b.walFile = f
	b.walPath = path
	if err := b.load(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return b, nil
}

// load restores beginOffset/currentOffset and the trailing window from disk.
func (b *replBacklog) load() error {
	stat, err := b.walFile.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()
	if size == 0 {
		return nil
	}
	tail := int64(0)
	if size > maxBacklogSize {
		tail = size - maxBacklogSize
	}
	if _, err := b.walFile.Seek(tail, 0); err != nil {
		return err
	}
	window := make([]byte, size-tail)
	if _, err := b.walFile.Read(window); err != nil {
		return err
	}
	b.buf = window
	b.beginOffset = tail
	b.currentOffset = size
	slog.Info("repl backlog restored from wal",
		"file", b.walPath,
		"begin_offset", b.beginOffset,
		"current_offset", b.currentOffset,
		"window_bytes", len(window))
	return nil
}

// appendBytes appends to the in-memory window and mirrors it to disk.
// Called under masterStatus.mu; disk writes are buffered by the OS.
func (b *replBacklog) appendBytes(bin []byte) {
	b.buf = append(b.buf, bin...)
	b.currentOffset += int64(len(bin))
	if b.walFile == nil {
		return
	}
	if _, err := b.walFile.Write(bin); err != nil {
		slog.Warn("write repl wal failed", "error", err)
	}
	if b.currentOffset-b.beginOffset > maxBacklogSize*2 {
		b.trimWal()
	}
}

// trimWal rewrites the persisted file keeping only the trailing window.
func (b *replBacklog) trimWal() {
	start := b.currentOffset - maxBacklogSize
	if start <= 0 {
		return
	}
	tmp := filepath.Join(filepath.Dir(b.walPath), walBacklogFilename+".tmp")
	out, err := os.Create(tmp)
	if err != nil {
		return
	}
	if _, err := out.Write(b.buf[maxBacklogSize-len(b.buf):]); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return
	}
	_ = out.Sync()
	_ = out.Close()
	_ = b.walFile.Close()
	_ = os.Rename(tmp, b.walPath)
	b.walFile, _ = os.OpenFile(b.walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	b.beginOffset = start
	slog.Info("repl wal trimmed", "begin_offset", b.beginOffset)
}

// close flushes and closes the persisted backlog file.
func (b *replBacklog) close() {
	if b.walFile != nil {
		_ = b.walFile.Sync()
		_ = b.walFile.Close()
		b.walFile = nil
	}
}

// newReplID returns a fresh replication id (40 hex chars, like Redis).
func newReplID() string {
	return utils.RandHexString(40)
}

// loadReplID reads a persisted master replid, or generates and persists a new
// one. Persisting the id keeps partial resync working across master restarts.
func loadReplID(walDir string) string {
	if walDir == "" {
		return newReplID()
	}
	path := filepath.Join(walDir, replidFilename)
	data, err := os.ReadFile(path)
	if err == nil && len(data) > 0 {
		return string(data)
	}
	id := newReplID()
	if err := os.WriteFile(path, []byte(id), 0644); err != nil {
		slog.Warn("persist repl id failed", "error", err)
	}
	return id
}

// removeReplPersistence drops the persisted backlog and replid when this node
// stops being a master.
func removeReplPersistence(walDir string) {
	if walDir == "" {
		return
	}
	_ = os.Remove(filepath.Join(walDir, walBacklogFilename))
	_ = os.Remove(filepath.Join(walDir, replidFilename))
}

// replBacklogDir returns the persistence directory for replication state.
func replBacklogDir() string {
	dir := config.Properties.Dir
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "repl")
}
