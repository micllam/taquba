# Contributing

## Layout

| Path | Contents |
|---|---|
| `taquba/` | The queue: the job records, the key layout, claims and leases, settlement, the scheduler, the reaper, the inbox and the read-only views |
| `taquba-workflow/` | The workflow runtime: runs of steps on a step queue, memoized side effects, typed jobs, run groups and retention sweeps |
| `taquba-cron/` | The cron scheduler, which enqueues a job at each firing of a schedule |
| `taquba-webhooks/` | Webhook delivery: a request enqueued as a job and a worker that sends it |
| `taquba-bencher/` | The benchmarks of the queue and the runtime, and their recorded results |
| `book/src/` | The book on the internals of the queue |
| `docs/decisions/` | The decision records |

The module docs of each crate describe its modules.

## Constraints

A change must preserve every constraint in this section.

- **Delivery.** Delivery is at least once, and a worker must be idempotent. See
  [ADR-0002](docs/decisions/0002-at-least-once-delivery.md).
- **Claim-only delivery.** `Queue::wait_for_jobs_on` only wakes a worker loop,
  and the loop takes its jobs through its own claim transaction. See
  [ADR-0003](docs/decisions/0003-claim-only-delivery.md).
- **One writer.** Only the process that opened a queue writes to its store.
  Another process reads through `QueueReader` and sends an `enqueue`, `cancel`
  or `signal` entry through the inbox. The inbox accepts only these three kinds
  and runs only toward the writer. A layer must receive the requests of another
  process as jobs on its queue. See
  [ADR-0005](docs/decisions/0005-one-writer-process.md).
- **On-disk layout.** The code reads only the current on-disk layout, and a
  minor release before 1.0 can break it. See
  [ADR-0004](docs/decisions/0004-layout-breaks-between-minor-releases.md).

## Checks

Before a change is reported as done, the commands of the `lint` and `test` jobs
in `.github/workflows/ci.yml` must pass. The `prose` workflow checks the
Markdown files and the Rust sources with Vale. After a clone, run `vale sync`,
which installs the styles that `.vale.ini` names.

## Decision records

A decision that constrains a future change must get a record in
`docs/decisions/`. The [README](docs/decisions/README.md) of that directory
states when a decision gets a record and gives the template.

## Versioning

The public types are exhaustive, and a breaking change must remove the replaced
item in the same release. A new enum variant, struct field or function parameter
in the public API must therefore go into a minor release.

## Book

The book in `book/src/` is part of the public surface. A change to the behaviour
or the API that a page describes must update the page in the same commit.

## Benchmarks

A change to a benchmark of `taquba-bencher` must update the crate's `README.md`
in the same commit: the catalogue row, a running example, the section on the
output format and any new environment variable.

## Releasing

The `Release` workflow publishes each crate whose version is absent from
crates.io, in dependency order. It then creates the tag `<crate>-v<version>` and
a GitHub release with the notes of the crate's CHANGELOG. `taquba-bencher` is
never published.

### The lockfile

Before the release commit, run `cargo update` and the commands of the `lint` and
`test` jobs in `.github/workflows/ci.yml`. Commit `Cargo.lock` on its own, with
the subject `root: update lockfile`.

### The release commit

1. Choose the version of each crate with unreleased entries.
2. Give a new version to each crate that depends on a crate with a new minor
   version. Add the entry "Raised the minimum `taquba` requirement to 0.N."
   under `### Changed` of its CHANGELOG.
3. In each CHANGELOG, add the heading `## [X.Y.Z] - YYYY-MM-DD` directly below
   `## [Unreleased]`. The workflow takes the first section after `Unreleased` as
   the release notes. Leave the `<!-- vale off -->` marker in place.
4. Set the version in the `Cargo.toml` of each crate and in
   `[workspace.dependencies]` of the root `Cargo.toml`.
5. Commit with a subject that lists the released crates, separated by commas,
   such as `release: taquba 0.16.0, taquba-workflow 0.15.0`. When the list makes
   the subject longer than 50 characters, the subject must list the first crate
   and end with `and others`. The body must have one line per released crate,
   such as `taquba 0.16.0`.

### Publishing

Once the release commit is on `master` and CI passes, start the `Release`
workflow on `master` from the Actions tab.
