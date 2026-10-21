# Releasing

The `Release` workflow publishes each crate whose version is absent from
crates.io, in dependency order. It then creates the tag `<crate>-v<version>` and
a GitHub release with the notes of the crate's CHANGELOG. `taquba-bencher` is
never published.

## The lockfile

Before the release commit, run `cargo update` and the commands of the `lint` and
`test` jobs in `.github/workflows/ci.yml`. Commit `Cargo.lock` on its own, with
the subject `root: update lockfile`.

## The release commit

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
   the subject longer than 50 characters, the subject lists the first crate and
   ends with `and others`. The body has one line per released crate, such as
   `taquba 0.16.0`.

## Publishing

Once the release commit is on `master` and CI passes, start the `Release`
workflow on `master` from the Actions tab.
