package ai

import (
	"strings"
	"sync"
)

var (
	embedderMu sync.RWMutex
	embedder   Embedder
	chunkOpts  = ChunkOptions{Size: DefaultChunkSize, Overlap: DefaultChunkOverlap}
)

// ChunkOptions bounds how a document is split for embedding.
type ChunkOptions struct {
	Size    int
	Overlap int
}

// SetEmbedder installs the process-wide embedder. Startup calls this once; until
// then DefaultEmbedder lazily builds the offline local embedder so tests and
// library users work without configuration.
func SetEmbedder(e Embedder) {
	embedderMu.Lock()
	defer embedderMu.Unlock()
	embedder = e
}

// SetChunkOptions installs the process-wide chunking bounds.
func SetChunkOptions(opts ChunkOptions) {
	embedderMu.Lock()
	defer embedderMu.Unlock()
	if opts.Size > 0 {
		chunkOpts.Size = opts.Size
	}
	if opts.Overlap >= 0 && opts.Overlap < chunkOpts.Size {
		chunkOpts.Overlap = opts.Overlap
	}
}

// DefaultEmbedder returns the configured embedder, building the local one on
// first use.
func DefaultEmbedder() Embedder {
	embedderMu.RLock()
	e := embedder
	embedderMu.RUnlock()
	if e != nil {
		return e
	}

	embedderMu.Lock()
	defer embedderMu.Unlock()
	if embedder == nil {
		embedder = NewLocalEmbedder(DefaultLocalDim)
	}
	return embedder
}

// DefaultChunkOptions returns the configured chunking bounds.
func DefaultChunkOptions() ChunkOptions {
	embedderMu.RLock()
	defer embedderMu.RUnlock()
	return chunkOpts
}

// ChunkWithDefaults splits text using the process-wide chunking bounds.
func ChunkWithDefaults(text string) []string {
	opts := DefaultChunkOptions()
	return Chunk(text, opts.Size, opts.Overlap)
}

// Chunk splits text into overlapping chunks ready for embedding.
//
// It is markdown-aware: blank-line-separated blocks are packed together up to
// size characters, and each chunk is prefixed with the heading it sits under, so
// a chunk lifted out of a long document still carries its section title.
func Chunk(text string, size, overlap int) []string {
	size, overlap = normalizeChunk(size, overlap)

	var (
		chunks   []string
		current  []string
		curLen   int
		hasFresh bool
	)
	flush := func() {
		if len(current) == 0 || !hasFresh {
			return
		}
		joined := strings.Join(current, "\n\n")
		chunks = append(chunks, joined)
		current, curLen, hasFresh = nil, 0, false
		if overlap > 0 {
			if tail := tailChars(joined, overlap); tail != "" {
				current = []string{tail}
				curLen = len(tail) + 2
			}
		}
	}

	for _, piece := range pieces(text, size) {
		if curLen > 0 && curLen+len(piece) > size {
			flush()
			// The carried overlap may leave no room for this piece. Dropping the
			// overlap beats emitting a chunk that overruns the requested size.
			if curLen > 0 && curLen+len(piece) > size {
				current, curLen = nil, 0
			}
		}
		current = append(current, piece)
		curLen += len(piece) + 2
		hasFresh = true
	}
	// Emit the remainder without leaving a dangling overlap-only chunk behind.
	if hasFresh {
		chunks = append(chunks, strings.Join(current, "\n\n"))
	}
	return chunks
}

func normalizeChunk(size, overlap int) (int, int) {
	if size <= 0 {
		size = DefaultChunkSize
	}
	if overlap < 0 || overlap >= size {
		overlap = size / 5
	}
	return size, overlap
}

// pieces returns the paragraphs of text, each prefixed with its heading
// breadcrumb and each no longer than size.
func pieces(text string, size int) []string {
	var out []string
	var headings []string
	var block []string

	flushBlock := func() {
		body := strings.TrimSpace(strings.Join(block, "\n"))
		block = nil
		if body == "" {
			return
		}
		prefix := ""
		if len(headings) > 0 {
			prefix = strings.Join(headings, " > ") + "\n"
		}
		for _, part := range splitOversized(prefix+body, size) {
			out = append(out, part)
		}
	}

	for _, line := range strings.Split(text, "\n") {
		if level, title, ok := heading(line); ok {
			flushBlock()
			if level-1 < len(headings) {
				headings = headings[:level-1]
			}
			headings = append(headings, title)
			continue
		}
		if strings.TrimSpace(line) == "" {
			flushBlock()
			continue
		}
		block = append(block, line)
	}
	flushBlock()
	return out
}

// heading reports whether a line is a markdown ATX heading.
func heading(line string) (level int, title string, ok bool) {
	trimmed := strings.TrimLeft(line, " ")
	if trimmed == "" || trimmed[0] != '#' {
		return 0, "", false
	}
	for level < len(trimmed) && trimmed[level] == '#' {
		level++
	}
	title = strings.TrimSpace(trimmed[level:])
	if title == "" || level > 6 {
		return 0, "", false
	}
	return level, title, true
}

// splitOversized breaks a piece that exceeds size, preferring sentence
// boundaries and falling back to a hard split so a wall of text without
// punctuation still produces bounded chunks.
func splitOversized(piece string, size int) []string {
	if len(piece) <= size {
		return []string{piece}
	}
	var out []string
	var buf strings.Builder
	for _, sentence := range splitSentences(piece) {
		if buf.Len() > 0 && buf.Len()+len(sentence) > size {
			out = append(out, strings.TrimSpace(buf.String()))
			buf.Reset()
		}
		if len(sentence) > size {
			if buf.Len() > 0 {
				out = append(out, strings.TrimSpace(buf.String()))
				buf.Reset()
			}
			for _, hard := range hardSplit(sentence, size) {
				out = append(out, hard)
			}
			continue
		}
		buf.WriteString(sentence)
	}
	if s := strings.TrimSpace(buf.String()); s != "" {
		out = append(out, s)
	}
	return out
}

// splitSentences keeps each terminator with the sentence it ends.
func splitSentences(text string) []string {
	var out []string
	start := 0
	for i, r := range text {
		switch r {
		case '.', '!', '?', '\n', ';':
			if r == '\n' {
				if seg := text[start:i]; strings.TrimSpace(seg) != "" {
					out = append(out, seg+"\n")
				}
			} else {
				out = append(out, text[start:i+1])
			}
			start = i + len(string(r))
		}
	}
	if start < len(text) {
		out = append(out, text[start:])
	}
	return out
}

func hardSplit(text string, size int) []string {
	runes := []rune(text)
	var out []string
	for len(runes) > 0 {
		n := min(size, len(runes))
		out = append(out, string(runes[:n]))
		runes = runes[n:]
	}
	return out
}

func tailChars(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	// Cut on a rune boundary so the overlap never starts mid-character.
	cut := len(s) - n
	for cut < len(s) && !isUTF8Start(s[cut]) {
		cut++
	}
	return s[cut:]
}

func isUTF8Start(b byte) bool { return b&0xC0 != 0x80 }
