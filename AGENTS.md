# Longhorn Manager: guide for contributors and coding agents

This file is the single source of truth for humans and coding agents (GitHub
Copilot, Claude, Cursor, etc.) working on `longhorn-manager`. When it disagrees
with a tool-specific file, this file wins. `.github/copilot-instructions.md`
only points here.

Read [Rules for changes](#rules-for-changes), [Build, test and validate](#build-test-and-validate)
and [Commits and pull requests](#commits-and-pull-requests) before changing
anything, and [Code review](#code-review) before reviewing a pull request. The
rest is a map of the codebase.

## Project overview

Longhorn Manager is the control plane of [Longhorn](https://github.com/longhorn/longhorn),
a distributed block storage system for Kubernetes. It runs as a DaemonSet
(`longhorn-manager`, one pod per node) and also provides the CSI driver, the
admission/conversion webhooks, the REST API, and several one-shot jobs. It
reconciles Longhorn CRDs (volumes, engines, replicas, nodes, backups, ...) and
drives the data plane (instance-manager, engine, replica, share-manager,
backing-image-manager) through the `engineapi` layer.

The main issue tracker is `longhorn/longhorn`, not this repository. Design context
for a change usually lives in the linked issue.

## Repository structure

| Directory | Purpose |
| --- | --- |
| `main.go`, `app/` | CLI entrypoint and subcommands: `daemon`, `csi`, `recurring-job`, `deploy-driver`, `pre-upgrade`, `post-upgrade`, `uninstall`, `system-rollout`, `global` |
| `controller/` | All reconcilers (one file per CRD, e.g. `volume_controller.go`, `engine_controller.go`, `node_controller.go`), `controller_manager.go` that wires them up, and shared `base_controller.go` / `instance_handler.go` |
| `controller/monitor/` | Long-running monitors (disk, snapshot, environment check, CPU policy) used by controllers; fake variants exist for tests |
| `datastore/` | Informer/lister-backed access layer for Longhorn and Kubernetes objects (`longhorn.go`, `kubernetes.go`, `uncached.go`). Controllers read and write resources through it |
| `types/` | Setting definitions (`setting.go`), shared constants, helpers, and condition types |
| `k8s/pkg/apis/longhorn/v1beta2/` | CRD Go types. `zz_generated.deepcopy.go` is generated |
| `k8s/pkg/client/` | Generated clientset, informers, listers |
| `k8s/crds.yaml` | Generated CRD manifest. Must stay in sync with the Go types |
| `engineapi/` | Clients for engine, instance-manager, proxy, backup, snapshot, backing-image and share-manager; engine simulator for tests |
| `api/` | REST API (router, handlers, resource models). `client/` is a generated Go client for it |
| `csi/` | CSI controller, node, identity and group-controller servers |
| `webhook/` | Admission (mutator/validator) and conversion webhooks, one package per resource under `webhook/resources/` |
| `scheduler/` | Replica and disk scheduling |
| `manager/` | Business logic behind the API (volume, node, backup, system backup/restore, ...) |
| `upgrade/` | Upgrade logic: API version migration and resource migration, one package per version range (e.g. `v19xto1100/`) |
| `metrics_collector/` | Prometheus metrics |
| `recovery_backend/` | Recovery backend for share-manager |
| `util/`, `constant/`, `meta/` | Utilities, constants, version metadata |
| `scripts/` | Build, test, validate, package scripts invoked by the Makefile and Dockerfile |
| `.github/` | CI workflows and `scripts/check_go_imports.py` |
| `vendor/` | Vendored dependencies (checked in) |

## Architecture notes

### Controller pattern

- Each controller embeds `*baseController` and uses a rate-limited workqueue.
  The `syncXxx(key)` function is the reconcile entrypoint. It must be idempotent
  and safe to retry.
- Read objects via the `datastore` (informer cache), mutate a deep copy, and write
  back with the datastore `Update*`/`UpdateStatus` helpers. Never mutate objects
  returned from the cache.
- Only the node that owns a resource reconciles it (`isResponsibleFor`-style
  checks). Cluster-wide work is done by a single "responsible" node, so
  changes in such code paths affect only one pod. Keep this in mind when
  checking for leaks or heavy loops.
- Instance lifecycle (engine/replica processes) goes through `instance_handler.go`
  and `engineapi`. Do not call instance-manager APIs from unrelated controllers.
- Controllers set Conditions and emit Kubernetes events; keep reasons and messages
  stable because users and tests depend on them.

### Settings

Settings are declared in `types/setting.go` (name, definition, default, validation)
and read through `datastore` (`GetSettingAs*`). Adding or changing a setting
usually also needs: the definition and default, validation in the setting webhook
and/or the setting controller, documentation in the `longhorn/longhorn` repo and a
test in `types/` or `datastore/`.

### CRDs and generated code

- Edit CRD types under `k8s/pkg/apis/longhorn/v1beta2/`, then run `make generate`
  to regenerate deepcopy, client code and `k8s/crds.yaml`.
- Never hand-edit `zz_generated.*`, `k8s/pkg/client/**`, `client/generated_*.go`
  or `k8s/crds.yaml`. CI (`scripts/validate`) fails when `crds.yaml` is stale.
- Changing a CRD field that already exists in released versions requires an upgrade
  path in `upgrade/` (and, for API version changes, the conversion webhook).

### Upgrade

`upgrade/upgrade.go` runs the version-specific migrations in order. New migrations
go in a new or the current `upgrade/vXXXtoYYY/` package and must be idempotent,
because the upgrade can be interrupted and re-run. Do not remove a migration unless
the version it covers is no longer upgradable.

### Webhooks

Each resource has its own package in `webhook/resources/<name>/` with a mutator and
a validator registered via `webhook/server`. Validation of user input belongs in
the validator; controllers must not rely on it as the only guard.

## Rules for changes

- Keep changes small and focused on the linked issue. Do not mix unrelated
  refactors with a fix.
- **Do not change behavior unintentionally.** For refactors and fixes, preserve
  existing logic, return values, error semantics, events and API/CRD output unless
  the issue says otherwise. When in doubt, add a test that pins the old behavior.
- Errors must not be dropped. Wrap them with actionable context using
  `github.com/cockroachdb/errors` (`errors.Wrapf`, `errors.Wrap`), which is what
  the existing code uses.
- Logging uses `logrus` entries (`sc.logger`, `WithError`, `WithField`). Do not add
  log spam in reconcile loops that run on every sync.
- Be careful with concurrency: avoid goroutine leaks, always propagate
  `context`/stop channels, and keep lock ordering consistent.
- Be careful with long-lived processes. The manager runs for months, so avoid
  anything that grows per sync (caches keyed by dynamic data, `reflect.StructOf`,
  unbounded maps, un-stopped timers/goroutines).
- Distinguish retryable errors (return the error so the workqueue retries) from
  terminal ones (log and `return nil`).
- Do not edit files under `vendor/` by hand. Change `go.mod`, then update the
  vendor directory with `go mod tidy && go mod vendor`. Dependency bumps are
  normally done by Renovate and are not part of feature/fix PRs.
- Do not add new dependencies without a clear need.
- Use `any` instead of `interface{}` in new code.

## Code style

- Format with `gofmt`/`go fmt`; `go vet` and `golangci-lint` must pass.
- Import groups are enforced by `.github/scripts/check_go_imports.py`. Within the
  `import (...)` block, separate groups with a blank line in this order:
  1. standard library
  2. third-party, non-Kubernetes
  3. Kubernetes (`k8s.io/...`, `sigs.k8s.io/...`) without alias
  4. Kubernetes with alias (e.g. `corev1 "k8s.io/api/core/v1"`)
  5. other Longhorn modules (`github.com/longhorn/...` except this repo)
  6. this repo (`github.com/longhorn/longhorn-manager/...`)

  In each group, no-alias imports come before aliased imports. Run the script
  locally on changed files when unsure:
  `git diff --name-only origin/master...HEAD -- '*.go' | python3 .github/scripts/check_go_imports.py`
- Comment only what needs clarification: non-obvious reasons, invariants, and
  workarounds. Do not narrate the code.
- Spell-check is enforced by `codespell` (`.codespellignore` lists exceptions).

## Build, test and validate

Official builds run in Docker (Go toolchain and golangci-lint are pinned in the
`Dockerfile`):

```bash
make            # default target: build + validate (CI artifacts)
make build      # build binaries into ./bin
make validate   # go vet, golangci-lint, go fmt, crds.yaml freshness check
make test       # run the unit tests in a container (needs privileged Docker)
make generate   # regenerate deepcopy/client code and k8s/crds.yaml
```

For quick iteration on a developer machine with Go installed (vendoring is on by
default because `vendor/` exists):

```bash
go build ./...
go vet ./controller/... ./util/...
go test ./util/ -run 'TestToMap' -count=1
go test ./controller/ -run 'TestGetCheckUpgradeRequestExtraInfo' -count=1
```

Always run the smallest relevant test selection first, then the whole package.
`scripts/test` runs with `-race` on amd64 and only tests packages touched by the
current diff when there are local changes.

Before sending a PR run at least: `go build ./...`, `go vet` on touched packages,
`gofmt -l` on touched files, and the tests of touched packages. If you changed
CRD types, also run `make generate` and commit the result.

## Testing

- Unit tests live next to the code (`*_test.go`). Both the standard `testing`
  package (with `testing`/`testify`) and `gopkg.in/check.v1` suites are used. Follow
  the style of the surrounding test file; `controller/` has a `check.v1` entry
  point in `controller_test.go`.
- Controller tests use the fake clients (`k8s.io/client-go/kubernetes/fake`,
  `k8s/pkg/client/clientset/versioned/fake`), the datastore with fake informers,
  and fakes from `controller/monitor` and `engineapi` (e.g. the engine simulator).
  Do not call real clusters or the network in unit tests.
- Prefer table-driven tests. Cover the success path, error paths and the edge case
  that motivated the change.
- A bug fix should come with a test that fails without the fix. If the fix is
  not unit-testable (e.g. needs a real cluster), state how it was verified in the
  PR.
- End-to-end behavior is tested in `longhorn/longhorn-tests`, not in this repo.

## Commits and pull requests

- Base branch is `master`. Backports to `v*` branches are created by Mergify
  from the PR, not by hand.
- Commit messages and PR titles follow Conventional Commits. Allowed types:
  `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `vendor`,
  `build`, `ci`, `revert`, `BREAKING`. Example:
  `fix(upgrade-checker): replace reflect.StructOf with map for deterministic field handling`
- Reference the issue in the commit body, such as `longhorn/longhorn#1234`.
- Sign off commits (`git commit -s`), following the existing history.
- Commits generated by a coding agent must end with a `Co-authored-by:` trailer
  naming the agent and model, with the vendor's noreply email. Examples:

  ```text
  Co-authored-by: Claude Opus 5.5 <noreply@anthropic.com>
  ```

  Keep the human author's `Signed-off-by:` line.
- Use the PR template. In "Which issue(s) this PR fixes", write
  `Issue #<number>` or `Issue longhorn/longhorn#<number>`. **Do not** use
  `Fixes`/`Closes` keywords, because they would auto-close the issue before QA.
- Keep the PR description honest about scope, risk and how it was tested.

## Code review

Act as an expert reviewer for distributed systems, storage, and cloud-native Go
control planes.

### Workflow

Follow this order strictly. Do not start line-by-line diff review before steps 1-4
are done.

1. Read the PR title and description.
2. Extract linked issue references (`Issue #1234`, `Issue longhorn/longhorn#1234`,
   an issue URL, `fixes/closes/resolves/refs`).
3. Read the linked issue(s) and confirm the problem statement, expected behavior
   and constraints.
4. Read the files modified in the diff and their direct callers/callees to learn
   the existing patterns.
5. Validate the implementation against the issue intent.
6. Perform the detailed code review and testing assessment.

### Source of truth and missing context

- The linked issue is the source of truth for intent.
- No issue linked: report `Missing issue reference`.
- Issue unavailable: report `Issue context unavailable`, perform a partial review
  limited to code correctness, and do not assess issue alignment.
- When context is missing, do not invent assumptions. Ask for it and limit the
  findings accordingly.

### What to check

Prioritize correctness, safety, stability and maintainability over style. Check
consistency with the existing controller/datastore/reconciliation patterns and flag
scope expansion or refactors unrelated to the issue. Most important first:

- **Correctness and safety:** data loss, races, deadlocks, goroutine leaks,
  missing context propagation, unsafe state transitions.
- **Unintended behavior change:** compare old and new code for return values,
  error semantics, events, conditions, API/CRD output and defaulting, especially in
  refactors. Say explicitly whether behavior is preserved.
- **Retry and idempotency:** reconcile and upgrade code must tolerate being
  re-run, and stale informer cache reads must not cause wrong writes.
- **Error handling:** no dropped errors, actionable context, correct
  retryable vs terminal handling, cleanup on failure paths.
- **Resource use:** unbounded loops, retries or caches, repeated expensive calls,
  anything that grows with uptime.
- **Scope:** flag refactors unrelated to the issue.
- **Tests:** the fix has a test that would fail without it; edge cases and error
  paths are covered.

Domain details to watch for:

- Concurrency and lifecycle: goroutine leaks, channel misuse (blocking, closing),
  context propagation, lock ordering and deadlock risk.
- Data consistency: idempotency, safe state transitions, stale cache handling, and
  duplicate side effects on retry.
- Error handling: errors must not be dropped; include actionable context; distinguish
  retryable from terminal errors.
- Resources: unbounded loops/retries, repeated expensive operations, missing cleanup
  on failure paths.

### Output format

Return the review in this order:

1. **Context Check**
2. **Issue Alignment**, one of:
   - `Fully aligned`: the implementation matches the linked issue intent and constraints.
   - `Partially aligned`: it addresses part of the intent but leaves meaningful gaps.
   - `Not aligned`: it does not solve the issue or conflicts with its intent.
   - `Unable to verify`: issue context is missing or unavailable.
3. **Key Findings**, ordered by severity:
   - Critical: correctness bugs, data loss risk, deadlock or race conditions.
   - Major: regression risk, missing error handling, unsafe state transitions.
   - Minor: maintainability issues, inconsistent patterns, suboptimal logic.
   - Informational: style and naming, reported last and briefly.
4. **Architectural and Logic Assessment**
5. **Testing Status**

Example:

```text
1. Context Check
The PR description references Issue longhorn/longhorn#4567 (improve replica rebuild
performance under high I/O). The PR changes replica_manager.go and spdk_io.go.

2. Issue Alignment
Partially aligned: the PR improves I/O throughput but does not address the
concurrent-rebuild edge cases mentioned in the issue.

3. Key Findings
- Critical: spdk_io.go:102-150 - Potential race in WriteAsync when multiple replicas
  rebuild simultaneously.
- Major: replica_manager.go:220-235 - The lock may block unrelated operations.
- Minor: replica_manager.go:310 - Naming inconsistency (replicaID vs replicaId).

4. Architectural and Logic Assessment
- Follows the existing SPDK integration patterns and keeps data plane separation.
- Lock handling could be refactored to avoid blocking unrelated operations.

5. Testing Status
- Unit tests were added for the new spdk_io.go functions.
- Missing tests for concurrent rebuilds, as highlighted in the issue.
```

### Behavioral constraints

- Begin findings with the most severe issue found, not with style or lint feedback.
- Justify each correctness or safety assessment with a specific reason tied to the
  code or the linked issue.
- Claim issue resolution only after citing the issue requirement and the code that
  satisfies it.
- Do not report issues that `gofmt`, `go vet` or `golangci-lint` already catch.

## Security

- Never commit secrets, tokens, kubeconfigs or backup target credentials.
- Do not log secret values (backup target credentials, encryption keys, CSI
  secrets). Log names, not contents.
- Report vulnerabilities through the Longhorn security policy
  (<https://github.com/longhorn/longhorn/security>), not in public issues or PRs.
