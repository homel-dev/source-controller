/*
Copyright 2026 The Flux authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package artifactmirror

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/fluxcd/pkg/apis/meta"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/opencontainers/go-digest"
)

type MirrorResult string

const (
	MirrorResultCreated        MirrorResult = "created"
	MirrorResultAlreadyPresent MirrorResult = "already-present"
	MirrorResultFailed         MirrorResult = "failed"
)

type Client interface {
	Ensure(ctx context.Context, namespace, name string, artifact meta.Artifact, file *os.File, size int64) (MirrorResult, error)
}

type minioClient struct {
	mc        *minio.Client
	bucket    string
	prefix    string
	clusterID string
}

func NewClient(cfg *Config) (Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	mc, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: !cfg.Insecure,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, err
	}

	return &minioClient{
		mc:        mc,
		bucket:    cfg.Bucket,
		prefix:    cfg.Prefix,
		clusterID: cfg.ClusterID,
	}, nil
}

func GenerateObjectKey(prefix, clusterID, namespace, name string, d digest.Digest) string {
	return path.Join(prefix, clusterID, "gitrepository", namespace, name, d.Algorithm().String(), d.Encoded()+".tar.gz")
}

func (c *minioClient) Ensure(ctx context.Context, namespace, name string, artifact meta.Artifact, file *os.File, size int64) (MirrorResult, error) {
	d, err := digest.Parse(artifact.Digest)
	if err != nil {
		return "", fmt.Errorf("invalid artifact digest: %w", err)
	}

	key := GenerateObjectKey(c.prefix, c.clusterID, namespace, name, d)

	if _, err = c.mc.StatObject(ctx, c.bucket, key, minio.StatObjectOptions{}); err == nil {
		return MirrorResultAlreadyPresent, nil
	} else if !isObjectNotFound(err) {
		errResp := minio.ToErrorResponse(err)
		if errResp.Code != "" {
			return "", fmt.Errorf("stat failed (s3 error %s): %w", errResp.Code, err)
		}
		return "", fmt.Errorf("stat failed (network/transport): %w", err)
	}

	opts := minio.PutObjectOptions{
		UserMetadata: map[string]string{
			"flux-kind":                "GitRepository",
			"flux-cluster":             c.clusterID,
			"flux-namespace":           namespace,
			"flux-name":                name,
			"flux-revision":            artifact.Revision,
			"flux-digest":              artifact.Digest,
			"flux-artifact-updated-at": artifact.LastUpdateTime.Format(time.RFC3339),
		},
		ContentType: "application/gzip",
		PartSize:    5 * 1024 * 1024,
	}

	if _, err = c.mc.PutObject(ctx, c.bucket, key, file, size, opts); err != nil {
		return "", fmt.Errorf("upload failed: %w", err)
	}

	return MirrorResultCreated, nil
}

func isObjectNotFound(err error) bool {
	errResp := minio.ToErrorResponse(err)
	switch errResp.Code {
	case "NoSuchKey", "NoSuchObject", "NotFound":
		return true
	default:
		return false
	}
}
