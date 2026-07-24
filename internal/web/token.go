package web

import (
	"math/rand/v2"
	"sync"
	"time"

	"log/slog"
)

const (
	// TokenLength is the length of generated auth tokens
	TokenLength = 128
	// DefaultTokenExpiryHours is the default token TTL
	DefaultTokenExpiryHours = 72
	// CleanupInterval is how often expired tokens are purged
	CleanupInterval = 10 * time.Minute
)

var uppercaseLetters = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

// TokenEntry represents an issued auth token.
type TokenEntry struct {
	Token     string     `json:"token"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"` // nil = permanent
}

// IsExpired returns true if the token has expired.
func (e *TokenEntry) IsExpired() bool {
	if e.ExpiresAt == nil {
		return false // permanent
	}
	return time.Now().After(*e.ExpiresAt)
}

// TokenEngine manages HTTP API auth tokens.
type TokenEngine struct {
	mu           sync.RWMutex
	tokens       map[string]*TokenEntry
	requirePass  string
	cleanupStop  chan struct{}
}

// NewTokenEngine creates a token engine for the given requirepass.
func NewTokenEngine(requirePass string) *TokenEngine {
	te := &TokenEngine{
		tokens:      make(map[string]*TokenEntry),
		requirePass: requirePass,
		cleanupStop: make(chan struct{}),
	}
	go te.cleanupLoop()
	return te
}

// Stop terminates the cleanup goroutine.
func (te *TokenEngine) Stop() {
	close(te.cleanupStop)
}

// GenerateToken creates a new 128-char random uppercase token.
// expiredHours: 0 means permanent.
func (te *TokenEngine) GenerateToken(expiredHours int) *TokenEntry {
	token := te.randomToken()

	var expiresAt *time.Time
	if expiredHours > 0 {
		t := time.Now().Add(time.Duration(expiredHours) * time.Hour)
		expiresAt = &t
	}

	entry := &TokenEntry{
		Token:     token,
		CreatedAt: time.Now(),
		ExpiresAt: expiresAt,
	}

	te.mu.Lock()
	te.tokens[token] = entry
	te.mu.Unlock()

	return entry
}

// ValidateToken checks if a token is valid (exists and not expired).
func (te *TokenEngine) ValidateToken(token string) bool {
	if token == "" {
		return false
	}
	te.mu.RLock()
	entry, ok := te.tokens[token]
	te.mu.RUnlock()
	if !ok {
		return false
	}
	if entry.IsExpired() {
		// Clean up expired token
		te.mu.Lock()
		delete(te.tokens, token)
		te.mu.Unlock()
		return false
	}
	return true
}

// RevokeToken removes a token from the engine.
func (te *TokenEngine) RevokeToken(token string) {
	te.mu.Lock()
	delete(te.tokens, token)
	te.mu.Unlock()
}

// Authenticate validates a password against requirepass and returns a token.
// Returns nil if the password is wrong.
// If no password is configured (requirePass == ""), a token is issued without password check.
func (te *TokenEngine) Authenticate(password string, expiredHours int) *TokenEntry {
	if te.requirePass == "" || password == te.requirePass {
		return te.GenerateToken(expiredHours)
	}
	return nil
}

// IsAuthConfigured returns true if requirepass is set.
func (te *TokenEngine) IsAuthConfigured() bool {
	return te.requirePass != ""
}

func (te *TokenEngine) randomToken() string {
	b := make([]rune, TokenLength)
	for i := range b {
		b[i] = uppercaseLetters[rand.IntN(len(uppercaseLetters))]
	}
	return string(b)
}

func (te *TokenEngine) cleanupLoop() {
	ticker := time.NewTicker(CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			te.cleanupExpired()
		case <-te.cleanupStop:
			return
		}
	}
}

func (te *TokenEngine) cleanupExpired() {
	te.mu.Lock()
	defer te.mu.Unlock()
	now := time.Now()
	for token, entry := range te.tokens {
		if entry.ExpiresAt != nil && now.After(*entry.ExpiresAt) {
			delete(te.tokens, token)
			slog.Debug("expired API token cleaned up")
		}
	}
}
