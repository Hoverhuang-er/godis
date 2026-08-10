package oss

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// gzipCompress compresses data with gzip. Payloads are intentionally NOT
// encrypted: bandwidth is the concern, confidentiality is not.
func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gzipDecompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("open gzip stream: %w", err)
	}
	defer r.Close()
	out, err := readAll(r)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func readAll(r io.Reader) ([]byte, error) {
	return io.ReadAll(r)
}
