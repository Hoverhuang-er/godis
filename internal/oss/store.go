// Package oss provides S3-compatible object storage access used for
// node-to-node data replication in cluster mode.
//
// Nodes replicate through a WAL (write-ahead log) of AOF command batches:
// the master appends new gzip-compressed segment objects (never deletes
// anything — S3 DELETE operations are billed and avoided), replicas pull
// segments after their last applied sequence and replay them. A small
// per-node state marker is overwritten (PUT) so disaster recovery can pick
// the node with the newest data purely from object metadata. Everything is
// compressed with gzip and intentionally NOT encrypted.
package oss

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config carries the S3-compatible connection options. It is populated from
// the [oss] section of the godis config file.
type Config struct {
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Prefix          string
	// ClusterID is the directory name under which all objects of this
	// cluster live (the cluster id is the S3 folder).
	ClusterID       string
	UseSSL          bool
	ForcePathStyle  bool
}

func (cfg *Config) validate() error {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return errors.New("oss.endpoint is required")
	}
	if strings.TrimSpace(cfg.Bucket) == "" {
		return errors.New("oss.bucket is required")
	}
	if strings.TrimSpace(cfg.AccessKeyID) == "" || strings.TrimSpace(cfg.SecretAccessKey) == "" {
		return errors.New("oss.access_key_id and oss.secret_access_key are required")
	}
	return nil
}

// Store wraps a minio S3-compatible client and the cluster object layout.
type Store struct {
	client *minio.Client
	cfg    Config
}

// NewStore creates a Store from cfg. It verifies the bucket exists and tries
// to create it when missing.
func NewStore(cfg Config) (*Store, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("create oss client: %w", err)
	}
	store := &Store{client: client, cfg: cfg}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	found, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket %s: %w", cfg.Bucket, err)
	}
	if !found {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{Region: cfg.Region}); err != nil {
			return nil, fmt.Errorf("create bucket %s: %w", cfg.Bucket, err)
		}
	}
	return store, nil
}

// Root returns the shared prefix under which all cluster objects live:
// {prefix}/{clusterID}. An empty cluster id falls back to "godis".
func (s *Store) Root() string {
	base := strings.Trim(s.cfg.Prefix, "/")
	dir := s.cfg.ClusterID
	if dir == "" {
		dir = "godis"
	}
	if base == "" {
		return dir
	}
	return base + "/" + dir
}

// WalDir returns the append-only WAL object directory of a node.
func (s *Store) WalDir(nodeAddr string) string {
	return fmt.Sprintf("%s/wal/%s", s.Root(), nodeAddr)
}

// StateDir returns the directory holding per-node state markers.
func (s *Store) StateDir() string {
	return fmt.Sprintf("%s/state", s.Root())
}

// BaseDir returns the directory holding optional base snapshots of a node.
func (s *Store) BaseDir(nodeAddr string) string {
	return fmt.Sprintf("%s/base/%s", s.Root(), nodeAddr)
}

// bucketExists is a small helper used by callers that need to probe
// liveness of a key without reading the payload.
func (s *Store) objectExists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.StatObject(ctx, s.cfg.Bucket, key, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	resp := minio.ToErrorResponse(err)
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, err
}
