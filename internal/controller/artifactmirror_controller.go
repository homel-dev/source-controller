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
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	"github.com/fluxcd/pkg/artifact/storage"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/fluxcd/source-controller/internal/artifactmirror"
	"github.com/opencontainers/go-digest"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ArtifactUpdatePredicate struct {
	predicate.Funcs
}

func (ArtifactUpdatePredicate) Create(event.CreateEvent) bool {
	return true
}

func (ArtifactUpdatePredicate) Update(e event.UpdateEvent) bool {
	oldRepo, okOld := e.ObjectOld.(*sourcev1.GitRepository)
	newRepo, okNew := e.ObjectNew.(*sourcev1.GitRepository)
	if !okOld || !okNew {
		return false
	}

	oldDigest := ""
	if oldRepo.Status.Artifact != nil {
		oldDigest = oldRepo.Status.Artifact.Digest
	}
	newDigest := ""
	if newRepo.Status.Artifact != nil {
		newDigest = newRepo.Status.Artifact.Digest
	}

	return oldDigest != newDigest ||
		oldRepo.Spec.Suspend != newRepo.Spec.Suspend ||
		!reflect.DeepEqual(oldRepo.Labels, newRepo.Labels) ||
		oldRepo.DeletionTimestamp.IsZero() != newRepo.DeletionTimestamp.IsZero()
}

// ArtifactMirrorReconciler asynchronously ensures that built artifacts are mirrored
// to a remote S3-compatible store without blocking the main GitRepository lifecycle.
type ArtifactMirrorReconciler struct {
	client.Client
	Storage       *storage.Storage
	Config        *artifactmirror.Config
	MirrorClient  artifactmirror.Client
	EventRecorder record.EventRecorder
}

func (r *ArtifactMirrorReconciler) SetupWithManager(mgr ctrl.Manager, concurrent int, rateLimiter workqueue.TypedRateLimiter[reconcile.Request]) error {
	if concurrent < 1 {
		return fmt.Errorf("artifact mirror concurrency must be greater than zero")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&sourcev1.GitRepository{}, builder.WithPredicates(ArtifactUpdatePredicate{})).
		Named("artifact_mirror").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: concurrent,
			RateLimiter:             rateLimiter,
		}).
		Complete(r)
}

func (r *ArtifactMirrorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var repo sourcev1.GitRepository
	if err := r.Get(ctx, req.NamespacedName, &repo); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if repo.Status.Artifact == nil || !repo.DeletionTimestamp.IsZero() || repo.Spec.Suspend {
		return ctrl.Result{}, nil
	}

	if r.Config.Selector != "" {
		sel, err := labels.Parse(r.Config.Selector)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("invalid mirror selector: %w", err)
		}
		if !sel.Matches(labels.Set(repo.Labels)) {
			return ctrl.Result{}, nil
		}
	}

	artifact := *repo.Status.Artifact
	d, err := digest.Parse(artifact.Digest)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("invalid artifact digest format: %w", err)
	}
	objectKey := artifactmirror.GenerateObjectKey(r.Config.Prefix, r.Config.ClusterID, req.Namespace, req.Name, d)

	logger := ctrl.LoggerFrom(ctx).WithValues(
		"operation", "artifact-mirror",
		"cluster", r.Config.ClusterID,
		"namespace", req.Namespace,
		"gitRepository", req.Name,
		"revision", artifact.Revision,
		"digest", artifact.Digest,
		"bucket", r.Config.Bucket,
		"objectKey", objectKey,
	)

	opCtx, cancel := context.WithTimeout(ctx, r.Config.OperationTimeoutDuration)
	defer cancel()

	localPath := r.Storage.LocalPath(artifact)
	file, err := os.Open(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ctrl.Result{}, fmt.Errorf("local artifact missing: %w", err)
		}
		return ctrl.Result{}, fmt.Errorf("failed to open artifact: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to stat artifact: %w", err)
	}

	verifier := d.Verifier()
	if _, err := io.Copy(verifier, file); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to hash local artifact: %w", err)
	}
	if !verifier.Verified() {
		return ctrl.Result{}, fmt.Errorf("local file digest does not match status digest %s", d.String())
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to rewind local artifact: %w", err)
	}

	start := time.Now()
	result, err := r.MirrorClient.Ensure(opCtx, req.Namespace, req.Name, artifact, file, info.Size())
	duration := time.Since(start)

	if err != nil {
		artifactmirror.MirrorAttempts.WithLabelValues(string(artifactmirror.MirrorResultFailed)).Inc()
		artifactmirror.MirrorDuration.WithLabelValues(string(artifactmirror.MirrorResultFailed)).Observe(duration.Seconds())
		logger.Error(err, "artifact mirror failed", "duration", duration.Seconds(), "bytes", info.Size())
		return ctrl.Result{}, fmt.Errorf("s3 mirror failed: %w", err)
	}

	artifactmirror.MirrorAttempts.WithLabelValues(string(result)).Inc()
	artifactmirror.MirrorDuration.WithLabelValues(string(result)).Observe(duration.Seconds())
	if result == artifactmirror.MirrorResultCreated {
		artifactmirror.MirrorBytes.Add(float64(info.Size()))
	}
	artifactmirror.MirrorLastSuccess.WithLabelValues(r.Config.ClusterID, req.Namespace, req.Name).SetToCurrentTime()

	logger.Info("artifact mirror ensured",
		"result", string(result),
		"duration", duration.Seconds(),
		"bytes", info.Size(),
	)

	return ctrl.Result{RequeueAfter: r.Config.RecheckIntervalDuration}, nil
}
