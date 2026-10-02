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
	"crypto/tls"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/fluxcd/pkg/apis/meta"
	mocks3 "github.com/fluxcd/source-controller/internal/mock/s3"
	. "github.com/onsi/gomega"
	"github.com/opencontainers/go-digest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGenerateObjectKey(t *testing.T) {
	g := NewWithT(t)

	d := digest.FromString("content")
	key := GenerateObjectKey("snapshots", "cluster-1", "flux-system", "repo", d)
	g.Expect(key).To(Equal("snapshots/cluster-1/gitrepository/flux-system/repo/" +
		d.Algorithm().String() + "/" + d.Encoded() + ".tar.gz"))

	other := digest.FromString("other")
	g.Expect(GenerateObjectKey("snapshots", "cluster-1", "flux-system", "repo", other)).NotTo(Equal(key))
	g.Expect(GenerateObjectKey("snapshots", "cluster-2", "flux-system", "repo", d)).NotTo(Equal(key))
	g.Expect(GenerateObjectKey("snapshots", "cluster-1", "other", "repo", d)).NotTo(Equal(key))
}

func TestClientEnsureAuthFailure(t *testing.T) {
	g := NewWithT(t)

	server := mocks3.NewServer("test-bucket")
	server.AccessKey = "expected-access"
	server.Start()
	defer server.Stop()

	cfg := testClientConfig(server)
	cfg.AccessKey = "wrong-access"
	client, err := NewClient(cfg)
	g.Expect(err).NotTo(HaveOccurred())

	artifact, file, size := testArtifactFile(t, []byte("artifact"))
	defer file.Close()

	_, err = client.Ensure(context.Background(), "default", "repo", artifact, file, size)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("stat failed"))
}

func TestClientEnsureMissingBucket(t *testing.T) {
	g := NewWithT(t)

	server := mocks3.NewServer("test-bucket")
	server.AccessKey = "test-access"
	server.Start()
	defer server.Stop()

	cfg := testClientConfig(server)
	cfg.Bucket = "missing-bucket"
	client, err := NewClient(cfg)
	g.Expect(err).NotTo(HaveOccurred())

	artifact, file, size := testArtifactFile(t, []byte("artifact"))
	defer file.Close()

	_, err = client.Ensure(context.Background(), "default", "repo", artifact, file, size)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("NoSuchBucket"))
}

func TestClientEnsureTLSFailure(t *testing.T) {
	g := NewWithT(t)

	server := mocks3.NewServer("test-bucket")
	server.AccessKey = "test-access"
	server.StartTLS(&tls.Config{})
	defer server.Stop()

	cfg := testClientConfig(server)
	cfg.Endpoint = strings.TrimPrefix(server.HTTPAddress(), "https://")
	cfg.Insecure = false

	client, err := NewClient(cfg)
	g.Expect(err).NotTo(HaveOccurred())

	artifact, file, size := testArtifactFile(t, []byte("artifact"))
	defer file.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = client.Ensure(ctx, "default", "repo", artifact, file, size)
	g.Expect(err).To(HaveOccurred())
}

func TestClientEnsureTimeout(t *testing.T) {
	g := NewWithT(t)

	server := mocks3.NewServer("test-bucket")
	server.AccessKey = "test-access"
	server.DelayNext = 250 * time.Millisecond
	server.Start()
	defer server.Stop()

	client, err := NewClient(testClientConfig(server))
	g.Expect(err).NotTo(HaveOccurred())

	artifact, file, size := testArtifactFile(t, []byte("artifact"))
	defer file.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err = client.Ensure(ctx, "default", "repo", artifact, file, size)
	g.Expect(err).To(HaveOccurred())
}

func testClientConfig(server *mocks3.Server) *Config {
	return &Config{
		Enabled:                  true,
		Endpoint:                 strings.TrimPrefix(server.HTTPAddress(), "http://"),
		Bucket:                   server.BucketName,
		Prefix:                   "snapshots",
		ClusterID:                "cluster-1",
		Insecure:                 true,
		Selector:                 "mirror=true",
		RecheckIntervalDuration:  time.Hour,
		OperationTimeoutDuration: time.Minute,
		AccessKey:                "test-access",
		SecretKey:                "test-secret",
	}
}

func testArtifactFile(t *testing.T, content []byte) (meta.Artifact, *os.File, int64) {
	t.Helper()
	g := NewWithT(t)

	path := t.TempDir() + "/artifact.tar.gz"
	g.Expect(os.WriteFile(path, content, 0o644)).To(Succeed())

	d := digest.FromBytes(content)
	artifact := meta.Artifact{
		Path:           "artifact.tar.gz",
		Revision:       "main/123",
		Digest:         d.String(),
		LastUpdateTime: metav1.Now(),
	}

	file, err := os.Open(path)
	g.Expect(err).NotTo(HaveOccurred())

	info, err := file.Stat()
	g.Expect(err).NotTo(HaveOccurred())
	return artifact, file, info.Size()
}
