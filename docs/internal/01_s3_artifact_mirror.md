S3 ARTIFACT MIRROR

Independent Reconciliation of Published Flux Artifacts to S3-Compatible Object Storage

Engineering Proposal

Namespace: source-controller • Owner: architecture-team

⸻

Navigation

* 0. Status, Scope, and Authority
* 1. Problem and Architecture Decision
* 2. Runtime Architecture and Ownership
* 3. Eligibility and Controller Behavior
* 4. Artifact and Object Contract
* 5. S3 Transport, Configuration, and Access
* 6. Failure, Recovery, and Retention
* 7. Memory Steward and Trust Boundary
* 8. Observability and Runtime Status
* 9. Test Contract
* 10. Build, CI, and Container
* 11. Repository and Upstream Contribution
* 12. Integration and Deployment Validation
* 13. Rollout, Rollback, and Implementation Phases
* 14. Acceptance, Scope, and Future Work
* 15. Final Target State and Canonical Rules
* 16. Closing Statement

⸻

0. Status, Scope, and Authority

Status: PROPOSAL
Audience: Maintainers, platform engineers, Flux contributors, Memory Steward integrators
Change policy: Canonical implementation proposal; no silent architecture or behavioral drift.
Revision: 2
Target: fluxcd/source-controller fork under homel-dev
Primary runtime target: S3-compatible object storage; Homel lab target is MinIO/AIStor
Upstream intent: Implement as an additive Flux capability and propose it upstream
Canonical document location: docs/internal/01_s3_artifact_mirror.md

This document describes intended behavior that is not yet implemented. Current runtime behavior remains defined by the checked-in source-controller implementation and executable tests.

0.1 Authority

This document is the canonical architecture and implementation contract for the Flux source artifact S3 mirror project.

Implementation MUST conform to this document.

If implementation work discovers that a requirement or architecture decision in this document is incorrect, the document MUST be updated in the same patch that changes the implementation.

Code, deployment configuration, tests, and operational procedures MUST NOT silently diverge from this document.

This revision supersedes the previous design that placed S3 mirroring directly into the GitRepositoryReconciler sub-reconciler chain.

The superseded design MUST NOT be implemented.

0.2 Organizational Policy

This project follows the organization-wide conventions defined by homel-dev/.github:

* docs/01_engineering_style_guide.md
* docs/02_documentation_style_guide.md
* docs/03_repository_conventions.md
* docs/04_ci_standard.md

The following requirements are especially relevant:

* correctness and reproducibility take precedence over convenience;
* architecture and implementation MUST agree;
* behavior changes MUST update documentation in the same patch;
* secrets MUST NOT be committed or logged;
* Kubernetes RBAC and NetworkPolicy MUST use least privilege;
* builds and validation SHOULD be reproducible from repository state;
* third-party GitHub Actions MUST be pinned to full commit SHAs;
* production images MUST use explicit tags or digests;
* domain-specific tests are required in addition to generic CI;
* verification claims MUST be based on commands actually executed.

The upstream fluxcd/source-controller repository contribution rules also apply to any code intended for upstream submission.

0.3 Reviewed Upstream Baseline

This design was verified against:

repository: fluxcd/source-controller
branch:     main
commit:     44ef8f4339a4a70878de5157f2e7366c3e9ae543

This was the current upstream main at the time this revision was produced.

Relevant implementation areas reviewed:

main.go
go.mod
AGENTS.md
internal/controller/gitrepository_controller.go
internal/controller/gitrepository_controller_test.go
internal/bucket/minio/minio.go
internal/bucket/minio/minio_test.go
internal/mock/s3/server.go
config/manager/deployment.yaml
Makefile

Relevant pinned dependency:

github.com/fluxcd/pkg/artifact v0.21.0

Relevant artifact storage implementation:

github.com/fluxcd/pkg/artifact/storage

Before implementation begins, the fork MUST be rebased or freshly created from the then-current upstream main, and this baseline section MUST be updated if upstream has moved.

Back to top

⸻

1. Problem and Architecture Decision

1.1 Problem Statement

Flux source-controller already performs the authoritative source-processing work:

1. resolve a GitRepository;
2. fetch the selected Git state;
3. compose included repositories where configured;
4. apply source-content configuration;
5. create the canonical Flux source artifact;
6. compute the artifact digest;
7. persist the artifact in local Flux storage;
8. publish the resulting status.artifact;
9. serve that artifact to Flux consumers.

The existing Memory Steward environment independently provides:

1. an S3-compatible bucket watched for repository snapshots;
2. an existing watcher;
3. an existing indexing pipeline;
4. repository indexing;
5. CodeGraph generation.

The required bridge is:

Ensure that the current completed Flux GitRepository artifact is also present as an immutable object in a configured S3-compatible bucket.

The project is a state-reconciliation problem, not an event-forwarding problem.

1.2 Design Decision

The S3 mirror MUST be implemented as a separate reconciler inside the existing source-controller process.

The selected architecture is:

GitRepositoryReconciler
    ->
publish status.artifact
ArtifactMirrorReconciler
    ->
observe published status.artifact
    ->
verify local artifact
    ->
ensure corresponding object exists in S3

The mirror MUST NOT be added as another step to the existing sequence:

reconcileStorage
reconcileSource
reconcileInclude
reconcileArtifact

This is a hard architecture requirement.

1.3 Why the Mirror Is a Separate Reconciler

The existing GitRepositoryReconciler intentionally short-circuits unchanged repositories.

On an unchanged revision, reconcileSource() may return an ignored result that causes the remaining sub-reconcilers to be skipped.

Therefore an appended reconcileArtifactMirror step would not provide steady-state retry.

It would violate the required recovery behavior:

artifact exists
MinIO unavailable
upload fails
Git remains unchanged
MinIO recovers
same artifact is eventually mirrored

A synchronous mirror step would also execute before the GitRepository status update is persisted.

Therefore S3 latency or failure could delay publication of a new Flux artifact and consume normal Git reconciliation workers.

The independent reconciler eliminates both problems.

1.4 Considered Alternatives

Option	Decision	Reason
Mirror sub-reconciler inside GitRepositoryReconciler	Rejected	Unchanged-source short circuit prevents reliable retry; S3 becomes part of the source reconciliation latency path
Independent reconciler in the same source-controller binary	Selected	Independent retry and worker pool without introducing another deployment
Separate mirror controller Deployment	Deferred fallback	Correct architecture but adds another deployable and operational component
Sidecar watching /data	Rejected	Couples to local filenames rather than published artifact state and does not naturally model same-revision/different-digest rebuilds
S3 FUSE/CSI as Flux storage	Rejected	Changes filesystem semantics and makes S3 emulate storage behavior Flux expects from a local filesystem
Notification relay	Rejected	Adds an unnecessary intermediate service and turns state reconciliation into event plumbing

The separate-controller alternative MAY be reconsidered only if maintaining the fork becomes materially more expensive than expected or upstream rejects the same-binary reconciler design.

Back to top

⸻

2. Runtime Architecture and Ownership

2.1 High-Level Architecture

flowchart LR
    Git[Git Remote]
    subgraph SC[source-controller]
        GR[GitRepositoryReconciler]
        LS[(Local Artifact Storage)]
        MR[ArtifactMirrorReconciler]
    end
    S3[(S3-compatible Bucket)]
    MW[Existing Memory Steward Watcher]
    IDX[Indexing Pipeline]
    CG[CodeGraph]
    KC[Flux Downstream Controllers]
    Git --> GR
    GR --> LS
    GR -->|status.artifact| MR
    LS --> KC
    MR -->|verify + ensure| S3
    S3 --> MW
    MW --> IDX
    IDX --> CG

Flux local artifact storage remains authoritative for Flux.

The S3 destination is an additional durable mirror used by external consumers.

2.2 Ownership Boundaries

2.2.1 GitRepositoryReconciler

Owns:

* Git fetch;
* reference resolution;
* source composition;
* source filtering;
* artifact construction;
* artifact digest;
* local artifact lifecycle;
* GitRepository.status.artifact;
* normal Flux readiness.

It MUST remain independent of S3 availability.

2.2.2 ArtifactMirrorReconciler

Owns:

* determining mirror eligibility;
* reading the published artifact identity;
* local artifact integrity validation;
* deterministic S3 object identity;
* S3 existence checks;
* uploading missing current artifacts;
* mirror retry and backoff;
* mirror-specific logging and metrics.

It MUST NOT fetch Git.

It MUST NOT create source archives.

It MUST NOT mutate repository contents.

2.2.3 S3-Compatible Object Storage

Owns:

* durable mirrored objects;
* object lifecycle policy;
* object-created notifications where configured.

It is not Flux’s authoritative source artifact storage.

2.2.4 Memory Steward

Owns:

* bucket observation;
* notification handling;
* deduplication;
* repository indexing;
* CodeGraph generation;
* consumer-side recovery from missed object notifications where required.

2.3 State Guarantee

This feature provides latest-state convergence.

It does not provide an immutable event log of every Git commit.

The required guarantee is:

For every eligible GitRepository, the artifact currently published in status.artifact MUST eventually exist in the configured S3 destination while that artifact remains current.

The implementation does not guarantee that every intermediate Git commit is mirrored.

Flux may itself observe multiple Git commits as one later state depending on polling intervals.

Controller work queues may also coalesce multiple status changes for the same Kubernetes object.

If artifact A becomes current and is replaced by artifact B before the mirror reconciler processes A, the required converged state is B.

This behavior is intentional.

Memory Steward requires current repository context, not a complete Git history.

Back to top

⸻

3. Eligibility and Controller Behavior

3.1 Mirror Eligibility

Mirroring MUST be explicitly opt-in.

A controller-wide destination MUST NOT automatically cause every GitRepository visible to the source-controller to cross the S3 trust boundary.

Eligibility SHOULD be controlled by a Kubernetes label selector configured in the mirror configuration.

Example intent:

memory.homel.dev/index=true

The exact Homel label name MAY differ.

The mirror reconciler MUST additionally reject:

* deleting objects;
* suspended objects where mirroring is explicitly disabled by policy;
* objects without status.artifact;
* objects outside the configured selector.

No GitRepository CRD field is required for the initial implementation.

3.2 Mirror Controller Behavior

The controller watches sourcev1.GitRepository.

The primary event trigger is a change to:

status.artifact.digest

The controller MUST also receive initial object events after process startup.

The controller MUST independently requeue successfully mirrored objects at a slow repair interval.

This periodic reconciliation exists to detect:

* externally deleted S3 objects;
* lifecycle-expired current objects;
* transient previous failures;
* controller restarts;
* drift between desired mirror state and object-store state.

Failure retries MUST use the controller work queue rate limiter.

The periodic successful-state recheck and failure backoff are separate mechanisms.

3.3 Controller Isolation

ArtifactMirrorReconciler MUST have its own controller-runtime controller and its own concurrency configuration.

Mirror operations MUST NOT consume the normal GitRepositoryReconciler worker budget.

Initial mirror concurrency SHOULD be conservative.

A default of one concurrent mirror reconcile is acceptable for the initial implementation.

The value MAY be made configurable after measurements demonstrate a need.

3.4 Time Bounds

All object-store operations MUST have a bounded parent context.

No S3 call may rely solely on SDK transport timeouts.

The mirror reconciliation MUST enforce an overall operation deadline covering:

HEAD / stat
verification preparation
PUT / multipart upload

A temporary object-store failure MUST terminate within the configured deadline and return control to the controller-runtime retry mechanism.

The implementation MUST NOT create a tight retry loop.

Exact production values MUST be recorded in deployment configuration rather than assumed by this document.

Back to top

⸻

4. Artifact and Object Contract

4.1 Artifact Identity

The S3 object identity MUST be based on the Flux artifact digest.

Git commit SHA alone is insufficient.

The same Git revision may produce a different Flux artifact when source-content configuration changes, including:

* spec.ignore;
* .sourceignore;
* spec.include;
* included artifact contents;
* spec.sparseCheckout;
* spec.recurseSubmodules;
* other inputs affecting artifact construction.

Therefore:

Git revision != complete artifact identity

and:

Flux Artifact digest == mirror content identity

for the purposes of this subsystem.

4.2 Canonical S3 Object Key

The object key MUST include a cluster scope to prevent collisions when several Flux installations use the same bucket.

Canonical layout:

<prefix>/<cluster-id>/gitrepository/<namespace>/<name>/<algorithm>/<digest>.tar.gz

Example:

repository-snapshots/minikube/gitrepository/flux-system/relentless-rekrow/sha256/8a1f...c92.tar.gz

The following properties MUST hold:

* deterministic;
* immutable for a given digest;
* independent of arrival order;
* independent of Git branch name;
* safe for arbitrary valid Kubernetes namespace/name values;
* globally scoped by configured cluster ID.

cluster-id MUST be explicitly configured.

It MUST NOT default silently to a value that could collide with another cluster.

4.3 Object Metadata

Mirrored objects SHOULD contain provenance metadata.

Required metadata:

flux-kind=GitRepository
flux-cluster=<cluster-id>
flux-namespace=<namespace>
flux-name=<name>
flux-revision=<artifact.revision>
flux-digest=<artifact.digest>
flux-artifact-updated-at=<artifact.lastUpdateTime>

The repository URL MUST NOT be copied into object metadata in the initial implementation.

This avoids accidental exposure of:

* URL userinfo;
* tokens;
* signed query parameters;
* private host topology.

If a future consumer requires source URLs, URL sanitization MUST be designed and tested separately.

Credentials or credential-derived values MUST NEVER appear in metadata.

4.4 Idempotency

Ensure() semantics MUST be used.

Conceptually:

desired key = key(artifact.digest)
if object exists:
    return AlreadyPresent
otherwise:
    upload exact artifact
    return Mirrored

Repeated reconciliations of the same digest MUST converge to the same object.

Two concurrent uploads of the same digest are acceptable provided both upload identical bytes.

Duplicate S3 object-created notifications MUST be treated as normal downstream behavior.

4.5 Mirror Result Contract

The internal mirror interface MUST distinguish at least:

Mirrored
AlreadyPresent
Failed

A plain error return is insufficient because metrics and logs must distinguish an actual upload from an idempotent no-op.

A possible internal shape is:

type MirrorResult string
const (
    MirrorResultCreated        MirrorResult = "created"
    MirrorResultAlreadyPresent MirrorResult = "already-present"
)
type ArtifactMirror interface {
    Ensure(
        ctx context.Context,
        identity SourceIdentity,
        artifact meta.Artifact,
        file *os.File,
    ) (MirrorResult, error)
}

This is illustrative rather than an API commitment.

The implementation MAY refine the types while preserving the semantic contract.

4.6 Local Artifact Consistency

The artifact path cannot be treated as an immutable path independent of reconciliation state.

A same-revision rebuild may replace the archive at the same local path with a new digest.

The mirror MUST therefore operate on a single opened file descriptor representing the bytes it intends to upload.

Required sequence:

read status.artifact
derive local path
open artifact file once
verify opened bytes against status.artifact.digest
seek back to beginning
upload from the same opened file

If the file does not exist or the digest does not match:

do not upload
return retryable reconciliation error

The next reconcile MUST reread the current GitRepository.status.artifact.

This prevents a race where status references artifact A while the filesystem path has already been replaced with artifact B.

4.7 Artifact Integrity

The exact bytes sent to S3 MUST match the Flux artifact digest.

The implementation MUST use Flux-compatible digest parsing and verification.

Storage.VerifyArtifact() demonstrates the existing Flux verification semantics and SHOULD be reused where safe.

However, correctness MUST ultimately be tied to the exact opened file used for upload.

Verification MUST NOT rely on hashing one file open and then independently reopening the path for upload if the path can change between those operations.

The final uploaded object MUST therefore originate from the already verified file descriptor.

S3 ETag MUST NOT be treated as equivalent to the Flux artifact digest.

Multipart ETags are not whole-object Flux digests.

Back to top

⸻

5. S3 Transport, Configuration, and Access

5.1 S3 Implementation

The existing repository already includes:

github.com/minio/minio-go/v7

and S3-compatible handling under:

internal/bucket/minio/

The initial implementation SHOULD reuse the same dependency and transport conventions.

It MUST NOT introduce a second S3 SDK without a demonstrated technical requirement.

The existing MinioClient was primarily designed for Flux Bucket source reads.

The mirror implementation MAY therefore:

1. refactor common S3 client construction into a neutral internal helper; or
2. add a dedicated internal mirror client using the existing minio-go/v7 dependency.

Existing BucketReconciler behavior MUST remain unchanged.

5.2 Multipart Uploads

The implementation MUST support artifacts large enough to trigger multipart upload.

Tests MUST NOT assume every repository snapshot is uploaded as a single S3 PUT.

The S3 implementation MUST:

* provide the artifact size to the client where supported;
* avoid unbounded buffering of entire repository artifacts in memory;
* abort failed multipart uploads where the SDK does not do so automatically;
* use the operation context deadline;
* close all opened files and streams.

The Memory Steward consumer contract MUST account for both normal object creation and completed multipart object creation.

5.3 Configuration Model

The initial Homel implementation SHOULD expose one optional controller configuration entry point rather than a collection of independent credential flags.

Preferred first-slice shape:

--artifact-mirror-config=<mounted-secret-file>

The referenced file is supplied by a Kubernetes Secret volume.

The configuration contains:

enabled
endpoint
bucket
prefix
clusterID
region
insecure
selector
recheckInterval
operationTimeout
accessKey
secretKey

TLS verification MUST be enabled by default.

insecure MUST default to false.

Example credentials in documentation MUST use obvious placeholders.

The exact public upstream configuration surface is NOT frozen by this internal design.

Before an upstream PR, the configuration contract MUST be discussed with Flux maintainers through an upstream issue.

The internal mirror implementation MUST therefore keep storage behavior decoupled from CLI/config parsing.

5.4 Credential Handling

Credentials MUST come from a Kubernetes Secret.

Credentials MUST NOT be:

* committed to Git;
* passed directly as plaintext command-line arguments;
* emitted in logs;
* emitted in Events;
* copied into object metadata.

The configuration MUST be validated before the mirror controller starts.

Missing or malformed credentials MUST fail mirror initialization with an actionable error.

An explicitly configured mirror MUST NOT silently fall back to anonymous S3 authentication because credential keys are missing.

Credential rotation semantics MUST be documented by the deployment implementation.

If the first implementation loads the Secret once at process startup, rotation requires a source-controller rollout and MUST be documented as such.

5.5 S3 Permissions

The mirror identity SHOULD receive only the permissions required for its prefix.

Expected capabilities:

ListBucket for the configured prefix
Get/Head object for the configured prefix
Put object for the configured prefix
multipart upload operations required by the SDK
AbortMultipartUpload where required

Delete permission is not required for the initial mirror.

Historical retention is an object-store lifecycle concern.

The policy MUST NOT grant administrative bucket permissions merely for convenience.

5.6 Network Policy

The Homel deployment MUST explicitly allow:

source-controller
    ->
configured S3/MinIO/AIStor endpoint

only on the required destination port.

A NetworkPolicy failure MUST behave as a bounded mirror failure.

It MUST NOT block Flux source reconciliation.

It MUST NOT indefinitely occupy mirror workers.

Back to top

⸻

6. Failure, Recovery, and Retention

6.1 Failure Semantics

The following are mirror failures:

* S3 endpoint unavailable;
* DNS failure;
* TLS failure;
* authentication failure;
* authorization failure;
* destination bucket missing;
* local artifact missing;
* local artifact digest mismatch;
* upload failure;
* multipart completion failure;
* operation deadline exceeded.

A mirror failure MUST NOT make the corresponding Flux artifact unusable.

A mirror failure MUST NOT change normal GitRepository Ready semantics in the initial implementation.

The mirror reconciler MUST return an error so controller-runtime applies independent retry/backoff.

The existing Flux source reconciliation path continues normally.

6.2 Recovery Semantics

This scenario is mandatory:

1. GitRepositoryReconciler creates artifact A.
2. status.artifact publishes A.
3. ArtifactMirrorReconciler observes A.
4. S3 is unavailable.
5. Mirror reconcile fails.
6. Git remains unchanged.
7. S3 becomes available.
8. ArtifactMirrorReconciler retries A.
9. A appears in S3.

No new Git commit may be required.

No modification of the GitRepository spec may be required.

No manual trigger may be required.

6.3 Periodic Repair Semantics

Successful mirroring does not permanently suppress reconciliation.

At the configured slow repair interval the controller MUST check whether the current object still exists.

If the current digest-addressed object has been externally deleted or expired by lifecycle policy, the mirror MUST restore it.

This restoration may produce another object-created notification.

Therefore the downstream consumer MUST be idempotent by artifact digest.

A cache stating that an object existed in the past MUST NOT prevent restoration of a currently missing object.

6.4 Historical Retention

Flux owns only the desired current mirror state.

S3 lifecycle policy owns historical object retention.

The mirror controller MUST NOT garbage-collect historical digest objects in the first implementation.

A configured bucket lifecycle MAY expire old objects.

The lifecycle policy MUST be chosen with the understanding that the currently published artifact will be restored by periodic reconciliation if it expires.

If historical retention requirements are added later, they MUST be documented separately.

Back to top

⸻

7. Memory Steward and Trust Boundary

7.1 Memory Steward Consumer Contract Gate

Before object schema and end-to-end acceptance tests are considered final, the existing Memory Steward watcher MUST be inspected.

The following behavior MUST be established from implementation or executed tests:

1. exact bucket and prefix watched;
2. supported S3 object-created event types;
3. behavior for multipart completion;
4. digest-based deduplication behavior;
5. ordering behavior when objects arrive out of order;
6. handling of duplicate notifications;
7. recovery behavior after missed notifications;
8. whether bucket listing/reconciliation exists in addition to event handling;
9. whether object metadata is consumed;
10. accepted archive format;
11. archive size limits;
12. protections against unsafe archive extraction;
13. expected repository identity encoding.

These are not assumptions.

They are a pre-integration verification gate.

If the watcher lacks reliable recovery from lost notifications, it SHOULD gain a periodic list-and-diff reconciliation path.

Such a compatibility change is a Memory Steward patch, not a reason to move mirroring back into the Flux critical path.

7.2 Consumer Ordering

Artifact digests are unique but not chronologically sortable.

The consumer MUST NOT infer recency from lexical S3 key order.

The mirrored metadata contains:

flux-artifact-updated-at
flux-revision
flux-digest

The exact rule by which Memory Steward chooses the active repository snapshot MUST be confirmed during the consumer contract gate.

A duplicate event for an already indexed digest MUST NOT regress or unnecessarily duplicate repository state.

7.3 Trust Boundary

This subsystem crosses a trust boundary:

Git repository content
    ->
Flux local artifact
    ->
shared object storage
    ->
Memory Steward
    ->
LLM-adjacent retrieval/indexing

Repository content MUST be treated as untrusted input.

The mirror itself MUST copy opaque artifact bytes only.

It MUST NOT interpret repository contents.

Archive extraction and indexing safety belong to Memory Steward and MUST include suitable controls for:

* path traversal;
* absolute paths;
* symlinks;
* archive bombs;
* oversized files;
* excessive extracted size;
* malformed archives.

The consumer contract review MUST confirm those protections.

Back to top

⸻

8. Observability and Runtime Status

8.1 Mirror Controller Observability

Observability is part of the first implementation slice.

Structured logs MUST include safe fields such as:

operation
cluster
namespace
gitRepository
revision
digest
bucket
objectKey
result
duration
bytes

Secrets and authenticated URLs MUST NOT be logged.

Metrics MUST cover at least:

attempts
created objects
already-present objects
failures
uploaded bytes
operation duration
last successful mirror time

Exact metric names MUST follow Flux naming conventions.

Failure logs MUST contain enough information to identify the repository and artifact without exposing credentials.

8.2 Kubernetes Events and Status

The first implementation MUST NOT add a new API status field or CRD condition unless operational evidence demonstrates that metrics and logs are insufficient.

This minimizes public API surface and avoids changing normal GitRepository status semantics.

A Kubernetes warning Event MAY be emitted for mirror failures if it can be implemented without uncontrolled event spam.

Upstream maintainers MAY request a different observability contract.

Back to top

⸻

9. Test Contract

9.1 Test Infrastructure

The existing:

internal/mock/s3/server.go

is currently read-oriented.

It does not provide sufficient test coverage for the required mirror behavior.

The project MUST budget explicit work to extend or replace the test S3 harness with support for:

* PUT;
* HEAD;
* multipart upload;
* object metadata capture;
* stored object bytes;
* authentication failure;
* forced server errors;
* delayed or blackholed requests where practical.

This is a real implementation work package.

It MUST NOT be assumed to already exist.

9.2 Unit Test Requirements

9.2.1 Object Identity

Verify:

same digest -> same object key
different digest -> different object key
same revision + different digest -> different object key
different cluster ID -> different object key
different namespace/name -> different object key

9.2.2 Metadata

Verify:

* expected provenance fields;
* no repository URL leakage;
* no credentials;
* valid metadata values;
* revision is metadata, not object identity.

9.2.3 S3 Ensure Behavior

Verify:

missing object -> upload -> Created
existing object -> no upload -> AlreadyPresent
upload bytes equal verified artifact bytes
metadata is stored
authentication failure -> error
missing bucket -> error
TLS failure -> error
context deadline -> bounded error

9.2.4 Integrity

Verify:

valid local artifact -> upload allowed
corrupted artifact -> upload refused
artifact replaced between status observation and open -> retry
opened artifact remains uploadable if path is replaced afterward

9.2.5 Multipart

Verify at least one artifact large enough to exercise multipart behavior.

The test MUST validate the completed object bytes.

9.3 Controller Test Requirements

The tests MUST exercise the actual mirror controller rather than only calling helper methods.

9.3.1 Case A — New Artifact

GitRepository status publishes digest A
    ->
mirror controller receives reconciliation
    ->
A appears in S3

9.3.2 Case B — Unchanged Artifact

digest A already exists
    ->
periodic or duplicate reconcile
    ->
AlreadyPresent
    ->
no duplicate object

9.3.3 Case C — S3 Outage

digest A published
    ->
S3 unavailable
    ->
mirror fails
    ->
GitRepository remains usable

9.3.4 Case D — Recovery Without Git Change

digest A published
    ->
first mirror fails
    ->
S3 recovers
    ->
controller retry succeeds

This is mandatory.

9.3.5 Case E — Controller Restart

digest A already published before process startup
    ->
new source-controller starts
    ->
initial informer event/reconciliation
    ->
A is ensured in S3

9.3.6 Case F — Same Revision, Different Digest

revision R, digest A
    ->
mirror A
source-content configuration changes
revision R, digest B
    ->
mirror B

9.3.7 Case G — Object Deleted After Success

A mirrored
    ->
A removed from S3
    ->
periodic repair
    ->
A restored

9.3.8 Case H — Newer Status While Old Work Is Queued

status changes A -> B
    ->
controller reconciles current object
    ->
B is the required converged state

The test MUST NOT require A to be preserved.

9.4 Critical Regression Test

A full-loop test MUST explicitly prove that the old rejected architecture would fail and the new architecture succeeds.

Required scenario:

GitRepository has unchanged revision
existing status.artifact is valid
mirror state is missing

Expected result:

GitRepositoryReconciler may no-op
ArtifactMirrorReconciler still runs independently
missing S3 object is restored

This test protects the primary architecture decision.

Back to top

⸻

10. Build, CI, and Container

10.1 Build Strategy

Only source-controller needs to be built for the first implementation.

The Flux CLI repository is not part of the initial build graph.

Primary binary target:

make manager

Expected binary:

build/bin/manager

Fast development validation SHOULD begin with targeted package tests.

The full upstream gate MUST run before declaring the implementation complete.

10.2 Required Verification Commands

Upstream-prescribed validation includes:

make tidy fmt vet
make test

The project MUST additionally run:

make verify
make manager
git status --short

Any generated files produced by required generation targets MUST be committed where upstream requires them.

A validation result MUST NOT be described as PASS unless the command was actually executed and its exit status observed.

Verification evidence SHOULD record:

command
exit code
stdout/stderr summary
source revision

10.3 CI

The fork MUST have CI.

CI MUST include the relevant upstream validation.

If Homel organization CI is layered on top, it MUST also use the organization reusable workflow according to homel-dev/.github policy.

Third-party Actions MUST be pinned to full commit SHAs.

Workflow permissions MUST be explicit and minimal.

CI for pull requests MUST NOT require production MinIO/AIStor credentials.

S3 behavior MUST use local deterministic test infrastructure.

10.4 Container Image

Only the custom source-controller image needs to be produced for the lab deployment.

Image versions MUST be traceable to source state.

Example development naming:

source-controller:s3-mirror-<git-sha>

Production deployment MUST use an explicit tag or digest.

latest is prohibited.

Back to top

⸻

11. Repository and Upstream Contribution

11.1 Repository Strategy

The project SHOULD use a fork:

upstream: fluxcd/source-controller
origin:   homel-dev/source-controller

Recommended development branch:

feat/s3-artifact-mirror

The fork MUST be maintained by rebasing on upstream.

Upstream main MUST NOT be merged into the feature branch.

The implementation SHOULD minimize changes to existing upstream files.

In particular, the selected architecture SHOULD avoid modifying the normal GitRepositoryReconciler except where absolutely necessary.

Preferred new-code shape:

internal/controller/artifactmirror_controller.go
internal/controller/artifactmirror_controller_test.go
internal/artifactmirror/...

Exact package structure MAY change after implementation review.

11.2 Upstream Change Surface

The desired upstream diff SHOULD primarily consist of:

new mirror reconciler
new mirror implementation
minimal manager wiring
minimal configuration plumbing
tests
documentation
metrics

The PR SHOULD NOT contain:

* RR-specific code;
* Memory Steward names;
* Homel-specific bucket names;
* hardcoded MinIO endpoints;
* unrelated refactoring;
* Flux CLI changes;
* new Git checkout logic.

The feature SHOULD be presented generically as:

Optional reconciliation of published Flux source artifacts into S3-compatible object storage without changing the authoritative local Flux artifact or normal source reconciliation path.

11.3 Upstream Issue Before PR

Flux contribution policy requests an issue discussing the problem and proposed solution when no issue already exists.

Before significant upstream-facing API/configuration polishing:

1. search for an existing relevant Flux issue;
2. if none exists, open a focused issue;
3. describe the problem;
4. describe the independent-reconciler architecture;
5. explain why the mirror is not in the source reconcile critical path;
6. ask for maintainer preference on public configuration shape and source-kind scope.

The issue MUST remain generic.

Memory Steward is an internal consumer and does not belong in the upstream problem statement.

11.4 Upstream Source-Kind Scope

The Homel requirement is initially:

GitRepository

The internal implementation SHOULD avoid unnecessary coupling that prevents eventual reuse for:

OCIRepository
HelmRepository
HelmChart
Bucket
ExternalArtifact

However, the first implementation MUST NOT expand scope merely for architectural symmetry.

The upstream issue SHOULD explicitly ask whether maintainers prefer:

* GitRepository only initially; or
* a generic mirror of any published Flux Artifact.

That decision belongs to upstream API design, not the Homel functional requirement.

11.5 DCO and AI Assistance

Flux requires DCO sign-off.

Only the human contributor may provide:

Signed-off-by:

AI-generated commits MUST NOT invent or append a human sign-off.

Flux requires AI assistance to be disclosed using:

Assisted-by: <agent>/<model>

Commit subjects MUST:

* use imperative mood;
* begin with a capital letter;
* contain no trailing period;
* remain concise;
* follow upstream length conventions.

Before final upstream merge, the branch MUST be rebased rather than merged with upstream main.

Back to top

⸻

12. Integration and Deployment Validation

12.1 Memory Steward Integration Validation

End-to-end validation is distinct from Flux unit/integration validation.

Required lab flow:

1. Eligible GitRepository receives a new effective source state.
2. GitRepositoryReconciler publishes a new artifact digest.
3. ArtifactMirrorReconciler observes that state.
4. Local artifact integrity is verified.
5. Digest-addressed object appears in S3.
6. Existing Memory Steward watcher observes the object.
7. Indexing begins.
8. Indexing completes.
9. CodeGraph reflects the mirrored artifact.

Evidence MUST separately demonstrate:

Flux source health
mirror health
object presence
watcher observation
indexing completion
CodeGraph availability

A running process alone does not establish any downstream state.

12.2 Failure Validation

The deployed implementation MUST exercise at least:

S3 unavailable
S3 recovery
invalid credentials
missing bucket
NetworkPolicy denial
TLS failure
duplicate reconciliation
duplicate S3 notification
same revision / different digest
large multipart artifact
source-controller restart
S3 object deletion after successful mirror
corrupted local artifact

Expected invariant:

S3 mirror failure does not block normal Flux GitOps operation.

Expected recovery invariant:

The currently published Flux artifact converges back into S3 without requiring a new Git commit.

12.3 Retention Validation

The deployed bucket lifecycle policy MUST be explicitly documented.

Validation MUST confirm what happens when:

historical object expires
current object expires
multipart upload is interrupted

Expected behavior for current-object expiry:

periodic mirror reconciliation detects absence
    ->
current object is recreated

Memory Steward MUST treat recreation of the same digest idempotently.

Back to top

⸻

13. Rollout, Rollback, and Implementation Phases

13.1 Rollout

Initial deployment SHOULD be narrow.

Recommended progression:

one test GitRepository
    ->
one real low-risk repository
    ->
selected Memory Steward repositories
    ->
full intended repository set

Eligibility selection SHOULD be controlled using labels rather than repeatedly changing controller deployment configuration.

During rollout, observe:

mirror latency
error rate
upload bytes
controller CPU/memory
S3 request volume
Memory Steward indexing behavior
duplicate indexing
GitRepository reconciliation latency

The rollout MUST confirm that the new controller does not measurably interfere with normal source reconciliation.

13.2 Rollback

Rollback MUST be simple.

Disabling the mirror MUST require only one of:

disable mirror configuration
remove the mirror selector
deploy upstream source-controller image

Rollback MUST NOT require modifying GitRepository source definitions.

Objects already mirrored into S3 MAY remain according to bucket retention policy.

Disabling the mirror MUST NOT affect the existing Flux artifact path.

13.3 Implementation Phases

13.3.1 Phase 0 — Verify Contracts

* refresh upstream source-controller;
* record baseline commit;
* inspect current Memory Steward watcher;
* record bucket/prefix/event/idempotency contract;
* record actual S3 implementation and version used in the lab;
* establish artifact size range from existing repositories.

Exit gate: no unresolved consumer assumption that changes object layout or notification handling.

13.3.2 Phase 1 — Fork and Clean Baseline

* create or refresh homel-dev/source-controller;
* configure origin and upstream;
* create feature branch;
* run upstream baseline validation;
* record results.

Exit gate: clean upstream baseline with documented verification.

13.3.3 Phase 2 — Minimal Mirror Core

Implement:

* artifact mirror interface;
* digest-based key generation;
* cluster-scoped identity;
* S3 configuration;
* S3 HEAD/PUT;
* exact-file digest verification;
* bounded operation context;
* result model.

Add unit tests.

Exit gate: local artifact can be deterministically and idempotently mirrored in tests.

13.3.4 Phase 3 — Independent Reconciler

Implement:

* ArtifactMirrorReconciler;
* digest-change watch;
* eligibility selector;
* own worker/rate limiter;
* retry behavior;
* periodic repair;
* manager wiring.

Add full controller tests.

Exit gate: recovery without a Git change is proven by an executed test.

13.3.5 Phase 4 — Observability

Implement:

* structured logs;
* attempt/result metrics;
* bytes;
* duration;
* last-success signal.

Exit gate: healthy, failed, retrying, and recovered mirror states can be distinguished operationally.

13.3.6 Phase 5 — Lab Deployment

* build custom manager;
* build custom image;
* deploy mirror configuration;
* apply NetworkPolicy;
* enable one selected repository.

Exit gate: real Flux artifact appears in S3 without affecting normal Flux consumers.

13.3.7 Phase 6 — Memory Steward Integration

* observe object-created handling;
* verify multipart behavior;
* verify deduplication;
* verify indexing;
* verify CodeGraph;
* exercise duplicate and recovery cases.

Exit gate: end-to-end current repository context converges automatically.

13.3.8 Phase 7 — Broaden Rollout

* enable selected repositories;
* observe resource usage;
* verify retention behavior;
* verify restart recovery.

Exit gate: stable lab operation.

13.3.9 Phase 8 — Upstream Preparation

* search existing issues;
* open upstream issue if required;
* incorporate maintainer feedback on public configuration and source-kind scope;
* remove any Homel-specific implementation detail from upstream-facing patch;
* rebase on current upstream main;
* run full upstream validation;
* prepare PR.

Back to top

⸻

14. Acceptance, Scope, and Future Work

14.1 Acceptance Criteria

The first project slice is complete only when all criteria below are demonstrated.

14.1.1 Architecture

* mirroring is implemented by an independent reconciler;
* normal GitRepositoryReconciler does not synchronously upload to S3;
* no additional Deployment is required;
* no second Git clone occurs;
* no second archive is constructed.

14.1.2 Correctness

* S3 identity is artifact-digest based;
* cluster scope prevents cross-cluster collisions;
* same revision with different digest creates distinct objects;
* exact uploaded bytes match the published artifact digest;
* corrupted local artifacts are not uploaded.

14.1.3 Independence

* S3 outage does not block Flux artifact publication;
* S3 outage does not consume normal Git reconcile workers;
* normal kustomize/helm consumers continue functioning.

14.1.4 Recovery

* failed upload retries without Git changes;
* source-controller restart reconciles previously published artifacts;
* deleted current S3 object is restored;
* periodic repair works.

14.1.5 Idempotency

* existing digest object produces no duplicate object;
* duplicate reconciliations are harmless;
* downstream duplicate events are harmless.

14.1.6 Consumer Integration

* Memory Steward observes uploaded snapshots;
* multipart-created objects are handled;
* indexing is digest-idempotent;
* current snapshot ordering is understood;
* CodeGraph reflects the mirrored artifact.

14.1.7 Verification

* targeted unit tests pass;
* controller integration tests pass;
* full upstream tests pass;
* make verify passes;
* manager binary builds;
* container image builds;
* working tree is clean after required generation;
* lab smoke tests pass with recorded evidence.

14.2 Explicit Non-Goals for the First Slice

The first slice does NOT include:

* replacing Flux local artifact storage with S3;
* mirroring every Git commit;
* using S3 as a Git history archive;
* adding a new Flux CRD;
* adding flux push s3;
* mirroring every Flux source kind;
* S3-side garbage collection by source-controller;
* changing Memory Steward indexing architecture;
* changing existing Flux readiness semantics;
* introducing another long-running service;
* building an OCI artifact around the tarball.

These items require a separate design decision if they become necessary.

14.3 Possible Later Work

After the first slice is stable, separate projects MAY evaluate:

14.3.1 flux push s3

A Flux CLI command for manually pushing artifacts to S3-compatible object storage.

This belongs in fluxcd/flux2 and MUST NOT be coupled to the runtime mirror implementation.

14.3.2 Generic Source Artifact Mirror

Extend the mirror reconciler from GitRepository to other Flux sources.

14.3.3 Multiple Destinations

Mirror one artifact to more than one object store.

14.3.4 Stronger Remote Verification

Read back and independently hash completed remote objects where required by threat model or compliance policy.

14.3.5 Historical Manifest/Index

Maintain a repository-level manifest of known digests if consumers require explicit history rather than current-state convergence.

None of these are required for the current project.

Back to top

⸻

15. Final Target State and Canonical Rules

15.1 Final Target State

sequenceDiagram
    participant Git as Git Remote
    participant GR as GitRepositoryReconciler
    participant FS as Flux Local Storage
    participant API as Kubernetes API
    participant MR as ArtifactMirrorReconciler
    participant S3 as S3 / MinIO / AIStor
    participant MW as Memory Steward Watcher
    participant IDX as Indexer / CodeGraph
    Git->>GR: source state
    GR->>FS: build canonical artifact
    GR->>API: publish status.artifact
    API-->>MR: GitRepository artifact state change
    MR->>FS: open current artifact
    MR->>MR: verify exact bytes against digest
    MR->>S3: HEAD digest-addressed object
    alt object missing
        MR->>S3: PUT verified artifact
        S3-->>MW: ObjectCreated
        MW->>IDX: index artifact
    else object already present
        MR->>MR: idempotent no-op
    end
    MR->>MR: schedule periodic repair

The intended runtime dependency graph is:

Git
  ->
Flux GitRepositoryReconciler
  ->
published Flux Artifact
  ->
independent ArtifactMirrorReconciler
  ->
S3-compatible object storage
  ->
existing Memory Steward watcher
  ->
existing indexing pipeline
  ->
CodeGraph

The Flux deployment path remains:

Git
  ->
Flux Artifact
  ->
kustomize-controller / helm-controller / other Flux consumers

and is not dependent on S3 or Memory Steward.

15.2 Canonical Implementation Rule

When implementation choices are ambiguous, prefer the choice that preserves these invariants:

Flux source reconciliation remains authoritative and independent.
Mirroring reconciles published state rather than participating in source creation.
Artifact digest defines mirrored content identity.
The exact verified local artifact bytes are uploaded.
S3 failure cannot block normal GitOps.
Current mirror state eventually converges without requiring a new Git commit.
The consumer is idempotent by artifact digest.
No additional service exists unless evidence proves one is required.

Any proposed implementation that violates one of these invariants requires an explicit revision of this document before code is merged.

Back to top

⸻

16. Closing Statement

This proposal defines the canonical implementation boundary for the S3 Artifact Mirror while the feature remains unimplemented.

The implementation MUST preserve Flux source reconciliation as the authoritative and independent GitOps path, MUST reconcile S3 state from the published Flux artifact rather than participate in source creation, and MUST preserve the artifact digest as the mirrored content identity.

The project is complete only when the code, tests, deployment configuration, operational validation, Memory Steward integration, and this document describe the same behavior without silent drift.

Back to top

⸻

END OF DOCUMENT
