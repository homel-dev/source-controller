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

package controller

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/artifact/storage"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/fluxcd/source-controller/internal/artifactmirror"
	mocks3 "github.com/fluxcd/source-controller/internal/mock/s3"
	. "github.com/onsi/gomega"
	"github.com/opencontainers/go-digest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestArtifactMirrorReconciler_Integration(t *testing.T) {
	g := NewWithT(t)

	mockS3 := mocks3.NewServer("test-bucket")
	mockS3.AccessKey = "test-access"
	mockS3.Start()
	defer mockS3.Stop()

	cfg := &artifactmirror.Config{
		Enabled:                  true,
		Endpoint:                 strings.TrimPrefix(mockS3.HTTPAddress(), "http://"),
		Bucket:                   "test-bucket",
		Prefix:                   "flux-mirror",
		ClusterID:                "cluster-test",
		Insecure:                 true,
		Selector:                 "mirror=true",
		RecheckIntervalDuration:  time.Hour,
		OperationTimeoutDuration: time.Minute,
		AccessKey:                "test-access",
		SecretKey:                "test-secret",
	}
	mc, err := artifactmirror.NewClient(cfg)
	g.Expect(err).NotTo(HaveOccurred())

	st := &storage.Storage{BasePath: t.TempDir()}
	contentA := []byte("artifact-A")
	artifactA := writeMirrorArtifact(t, st, "default", "test-repo-mirror", "main/123", contentA)

	repo := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-repo-mirror",
			Namespace: "default",
			Labels:    map[string]string{"mirror": "true"},
		},
		Spec: sourcev1.GitRepositorySpec{
			URL:      "https://github.com/example/repo",
			Interval: metav1.Duration{Duration: time.Minute},
		},
		Status: sourcev1.GitRepositoryStatus{
			Artifact: &artifactA,
		},
	}

	s := runtime.NewScheme()
	g.Expect(sourcev1.AddToScheme(s)).To(Succeed())
	k8sClient := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&sourcev1.GitRepository{}).
		WithObjects(repo).
		Build()

	r := &ArtifactMirrorReconciler{
		Client:       k8sClient,
		Storage:      st,
		Config:       cfg,
		MirrorClient: mc,
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: repo.Name, Namespace: repo.Namespace}}

	// New artifact.
	res, err := r.Reconcile(context.Background(), req)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(res.RequeueAfter).To(Equal(cfg.RecheckIntervalDuration))
	keyA := mirrorObjectKey(t, cfg, repo.Namespace, repo.Name, artifactA.Digest)
	objectA := mockS3.Object(keyA)
	g.Expect(objectA).NotTo(BeNil())
	g.Expect(objectA.Content).To(Equal(contentA))
	g.Expect(objectA.UserMetadata["flux-revision"]).To(Equal("main/123"))

	// Idempotent AlreadyPresent.
	res, err = r.Reconcile(context.Background(), req)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(res.RequeueAfter).To(Equal(cfg.RecheckIntervalDuration))
	g.Expect(mockS3.ObjectCount(keyA)).To(Equal(1))

	// Same revision, different digest. The queued request must converge to the
	// currently published artifact, not the previously observed one.
	contentB := []byte("artifact-B")
	artifactB := writeMirrorArtifact(t, st, repo.Namespace, repo.Name, "main/123", contentB)
	updateMirrorStatus(t, k8sClient, req.NamespacedName, artifactB)
	res, err = r.Reconcile(context.Background(), req)
	g.Expect(err).NotTo(HaveOccurred())
	keyB := mirrorObjectKey(t, cfg, repo.Namespace, repo.Name, artifactB.Digest)
	g.Expect(keyB).NotTo(Equal(keyA))
	objectB := mockS3.Object(keyB)
	g.Expect(objectB).NotTo(BeNil())
	g.Expect(objectB.Content).To(Equal(contentB))

	// S3 outage followed by recovery without any GitRepository change.
	mockS3.DeleteObject(keyB)
	mockS3.FailNext = true
	_, err = r.Reconcile(context.Background(), req)
	g.Expect(err).To(HaveOccurred())
	g.Expect(mockS3.Object(keyB)).To(BeNil())

	res, err = r.Reconcile(context.Background(), req)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(res.RequeueAfter).To(Equal(cfg.RecheckIntervalDuration))
	objectB = mockS3.Object(keyB)
	g.Expect(objectB).NotTo(BeNil())
	g.Expect(objectB.Content).To(Equal(contentB))

	// A fresh reconciler instance restores/ensures an already-published artifact
	// without requiring a Git change.
	restarted := &ArtifactMirrorReconciler{
		Client:       k8sClient,
		Storage:      st,
		Config:       cfg,
		MirrorClient: mc,
	}
	res, err = restarted.Reconcile(context.Background(), req)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(res.RequeueAfter).To(Equal(cfg.RecheckIntervalDuration))
	g.Expect(mockS3.ObjectCount(keyB)).To(Equal(1))

	// Periodic repair restores a deleted current object. This also protects the
	// unchanged-source regression: GitRepositoryReconciler does not need to run.
	mockS3.DeleteObject(keyB)
	res, err = r.Reconcile(context.Background(), req)
	g.Expect(err).NotTo(HaveOccurred())
	objectB = mockS3.Object(keyB)
	g.Expect(objectB).NotTo(BeNil())
	g.Expect(objectB.Content).To(Equal(contentB))

	// Corrupted local bytes are rejected even if the remote object exists.
	g.Expect(os.WriteFile(st.LocalPath(artifactB), []byte("corrupt"), 0o644)).To(Succeed())
	_, err = r.Reconcile(context.Background(), req)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("local file digest does not match"))

	// Multipart upload preserves exact bytes.
	largeContent := bytes.Repeat([]byte("A"), 11*1024*1024)
	artifactC := writeMirrorArtifact(t, st, repo.Namespace, repo.Name, "main/456", largeContent)
	updateMirrorStatus(t, k8sClient, req.NamespacedName, artifactC)
	res, err = r.Reconcile(context.Background(), req)
	g.Expect(err).NotTo(HaveOccurred())
	keyC := mirrorObjectKey(t, cfg, repo.Namespace, repo.Name, artifactC.Digest)
	objectC := mockS3.Object(keyC)
	g.Expect(objectC).NotTo(BeNil())
	g.Expect(objectC.Content).To(Equal(largeContent))
}

func TestArtifactMirrorReconciler_UsesVerifiedOpenFile(t *testing.T) {
	g := NewWithT(t)

	st := &storage.Storage{BasePath: t.TempDir()}
	original := []byte("verified-original")
	replacement := []byte("replacement-after-open")
	artifact := writeMirrorArtifact(t, st, "default", "race-repo", "main/1", original)
	localPath := st.LocalPath(artifact)

	repo := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "race-repo",
			Namespace: "default",
			Labels:    map[string]string{"mirror": "true"},
		},
		Status: sourcev1.GitRepositoryStatus{Artifact: &artifact},
	}

	s := runtime.NewScheme()
	g.Expect(sourcev1.AddToScheme(s)).To(Succeed())
	k8sClient := fake.NewClientBuilder().WithScheme(s).WithObjects(repo).Build()

	var uploaded []byte
	mirrorClient := mirrorClientFunc(func(_ context.Context, _, _ string, _ meta.Artifact, file *os.File, size int64) (artifactmirror.MirrorResult, error) {
		g.Expect(os.Rename(localPath, localPath+".old")).To(Succeed())
		g.Expect(os.WriteFile(localPath, replacement, 0o644)).To(Succeed())

		var err error
		uploaded, err = io.ReadAll(file)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(size).To(Equal(int64(len(original))))
		return artifactmirror.MirrorResultCreated, nil
	})

	cfg := &artifactmirror.Config{
		Enabled:                  true,
		Bucket:                   "test-bucket",
		Prefix:                   "flux-mirror",
		ClusterID:                "cluster-test",
		Selector:                 "mirror=true",
		RecheckIntervalDuration:  time.Hour,
		OperationTimeoutDuration: time.Minute,
	}

	r := &ArtifactMirrorReconciler{
		Client:       k8sClient,
		Storage:      st,
		Config:       cfg,
		MirrorClient: mirrorClient,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: repo.Name, Namespace: repo.Namespace},
	})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(uploaded).To(Equal(original))
	current, err := os.ReadFile(localPath)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(current).To(Equal(replacement))
}

func TestArtifactUpdatePredicate(t *testing.T) {
	g := NewWithT(t)
	p := ArtifactUpdatePredicate{}

	base := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "repo",
			Namespace: "default",
			Labels:    map[string]string{"mirror": "false"},
		},
		Status: sourcev1.GitRepositoryStatus{
			Artifact: &meta.Artifact{Digest: digest.FromString("a").String()},
		},
	}

	g.Expect(p.Create(event.CreateEvent{Object: base})).To(BeTrue())

	labelChanged := base.DeepCopy()
	labelChanged.Labels = map[string]string{"mirror": "true"}
	g.Expect(p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: labelChanged})).To(BeTrue())

	suspendChanged := base.DeepCopy()
	suspendChanged.Spec.Suspend = true
	g.Expect(p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: suspendChanged})).To(BeTrue())

	digestChanged := base.DeepCopy()
	digestChanged.Status.Artifact = &meta.Artifact{Digest: digest.FromString("b").String()}
	g.Expect(p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: digestChanged})).To(BeTrue())

	annotationOnly := base.DeepCopy()
	annotationOnly.Annotations = map[string]string{"example": "value"}
	g.Expect(p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: annotationOnly})).To(BeFalse())
}

type mirrorClientFunc func(context.Context, string, string, meta.Artifact, *os.File, int64) (artifactmirror.MirrorResult, error)

func (f mirrorClientFunc) Ensure(ctx context.Context, namespace, name string, artifact meta.Artifact, file *os.File, size int64) (artifactmirror.MirrorResult, error) {
	return f(ctx, namespace, name, artifact, file, size)
}

func writeMirrorArtifact(t *testing.T, st *storage.Storage, namespace, name, revision string, content []byte) meta.Artifact {
	t.Helper()
	g := NewWithT(t)

	artifact := meta.Artifact{
		Path:     filepath.Join("gitrepository", namespace, name, "artifact.tar.gz"),
		Revision: revision,
	}
	g.Expect(st.MkdirAll(artifact)).To(Succeed())
	g.Expect(st.AtomicWriteFile(&artifact, bytes.NewReader(content), 0o644)).To(Succeed())
	return artifact
}

func updateMirrorStatus(t *testing.T, c client.Client, key types.NamespacedName, artifact meta.Artifact) {
	t.Helper()
	g := NewWithT(t)

	var repo sourcev1.GitRepository
	g.Expect(c.Get(context.Background(), key, &repo)).To(Succeed())
	repo.Status.Artifact = &artifact
	g.Expect(c.Status().Update(context.Background(), &repo)).To(Succeed())
}

func mirrorObjectKey(t *testing.T, cfg *artifactmirror.Config, namespace, name, digestString string) string {
	t.Helper()
	g := NewWithT(t)
	d, err := digest.Parse(digestString)
	g.Expect(err).NotTo(HaveOccurred())
	return artifactmirror.GenerateObjectKey(cfg.Prefix, cfg.ClusterID, namespace, name, d)
}
