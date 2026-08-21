# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.2] - 2026-08-21

Monthly update.

### Security

- Updated `h2` to a patched version for RUSTSEC-2026-0258. The HTTP/2 stack used for S3 requests accepted and queued empty DATA frames without limit, which could grow memory usage without bound or panic on overflow if streams were not drained.

### Changed

- aws-sdk-s3 `v1.140.0 -> v1.143.0`
- Updated other dependencies

## [1.3.1] - 2026-08-08

### Fixed

- Command line validation error messages (e.g. `error: --recursive is not valid for bucket listing`) now end with a newline. Previously the message was printed without a trailing newline, so the next shell prompt appeared on the same line.

## [1.3.0] - 2026-08-05

### Fixed

- [Breaking change] A listing interrupted by Ctrl+C (SIGINT) now exits with code 130 (128 + SIGINT, the conventional shell encoding for termination by signal) instead of 0. Previously an interrupted listing was indistinguishable from a successful one by its exit code; scripts that test the exit status should treat 130 as user interruption rather than success.

## [1.2.2] - 2026-08-03

### Fixed

- Generating a shell completion script into a closed pipe (e.g. `s3ls --auto-complete-shell bash | head -1` when the reader exits without consuming the script) no longer panics with `failed to write completion file: Broken pipe`. A closed pipe is now treated as the normal end of a pipeline and the command exits 0; any other write failure (e.g. disk full on a redirect) exits 1 with an error message. Object and bucket listing output already handled broken pipes and are unchanged.

## [1.2.1] - 2026-07-26

### Changed

- aws-sdk-s3 `v1.138.1 -> v1.140.0`
- Updated other dependencies

## [1.2.0] - 2026-07-25

### Changed

- [Breaking change] The positional target argument no longer reads its value from the `TARGET` environment variable. This generic variable name could unintentionally override the command line arguments.

## [1.1.0] - 2026-07-20

Monthly update.

### Fixed

- Parallel object listing now honors `--max-parallel-listings` for deep-prefix (leaf) listings. Previously the concurrency permit was released before the sequential scan of each leaf prefix, so buckets with many deep prefixes could issue an unbounded number of concurrent `ListObjectsV2` calls regardless of the configured limit.
- `--rate-limit-api` now enforces the exact requested rate. Previously the effective rate was rounded down to a multiple of 10 (e.g. `--rate-limit-api 19` throttled to 10 requests per second).

### Security

- `--help` no longer prints the *values* of the credential environment variables `TARGET_ACCESS_KEY`, `TARGET_SECRET_ACCESS_KEY`, and `TARGET_SESSION_TOKEN`, preventing credentials from leaking into help output when they are set in the environment. The variable names are still shown.
- Bumped `crossbeam-epoch` to `v0.9.20` to resolve RUSTSEC-2026-0204.

### Changed

- aws-sdk-s3 `v1.137.0 -> v1.138.1`
- MSRV `1.91.1 -> 1.94.1`
- Updated other dependencies

## [1.0.3] - 2026-06-27

Monthly update.

### Changed

- aws-sdk-s3 `v1.133.0 -> v1.137.0`
- Updated other dependencies

## [v1.0.2] - 2026-05-25

Monthly update.

### Changed

- aws-sdk-s3 `v1.131.0 -> v1.133.0`
- Updated other dependencies

## [v1.0.1] - 2026-05-06

### Fixed

- `ListBuckets` no longer sends the `MaxBuckets` parameter when a custom endpoint (`--target-endpoint-url`) is configured, improving compatibility with S3-compatible storage providers that do not support this parameter. Default AWS S3 behavior is unchanged.

## [v1.0.0] - 2026-04-27

### Changed

- First stable release. No code changes since v0.4.1; version bumped to 1.0.0 to signal API/CLI stability under [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.4.1] - 2026-04-26

### Changed

- README: add a Non-Goals section explicitly listing out-of-scope items (object/bucket modification, per-object `HeadObject`/`GetObject`, APIs beyond `ListObjectsV2`/`ListObjectVersions`/`ListBuckets`, glob/wildcard expansion, cross-tool compatibility, plugin mechanism), and align the example tool list in Scope to match.

## [v0.4.0] - 2026-04-25

### Changed

- **BREAKING:** Whitespace-aligned columns are now the default object/bucket listing format. The previous default (tab-separated text) is now opt-in via `--tsv`. Scripts that parsed the default output with `cut`, `awk`, or `IFS=$'\t'` must add `--tsv`.

### Added

- `--tsv` — emit tab-separated text instead of the default whitespace-aligned columns. Composes with `--no-sort`, `--header`, `--summarize`, and every `--show-*` flag. Cannot be combined with `--json`.

### Removed

- `--aligned` — replaced by the new default. The flag is rejected with an error to fail fast on existing scripts; remove it (and add `--tsv` if tab-separated output is required).

## [v0.3.0] - 2026-04-19

### Added

- `--aligned` — display output with whitespace-padded columns that line up on screen, independent of `--human-readable` (which formats individual values). Composes with `--no-sort`, `--header`, `--summarize`, and every `--show-*` flag; conflicts with `--json`. Works on both object and bucket listings.
- `-1` / `--one` — print just the key (or bucket name) per line, `ls -1`-style. All `--show-*` columns are ignored. With `--show-objects-only`, common prefixes are suppressed. With `--header`, a single `KEY`/`BUCKET` label is emitted. Conflicts with `--json`.
- `--target-no-sign-request` — read public (anonymous) S3 buckets without loading credentials. Requests are sent unsigned. Conflicts with `--target-profile`, `--target-access-key`, `--target-secret-access-key`, and `--target-session-token`.

### Changed

- Updated dependencies to latest compatible versions.
- Internal refactor of the display layer and expanded test coverage.

### Fixed

- Bucket and object listing errors now include the underlying cause from the AWS SDK error source chain, replacing terse top-level messages such as `dispatch failure` (e.g. surfacing the missing-profile detail behind a `--target-profile` typo).

### Security

- Removed transitive dependency on the vulnerable `rustls 0.21` / `rustls-webpki 0.101.x` (RUSTSEC-2026-0098) by disabling the legacy `rustls` default feature on `aws-config` and `aws-sdk-s3`. TLS now goes through the modern `default-https-client` path (`rustls 0.23`).

## [v0.2.0] - 2026-04-13

### Added

- Automated dependency auditing and code analysis via GitHub Actions.

### Fixed

- Fix incorrect documentation about why parallel listing is disabled for Express One Zone directory buckets.

### Changed

- Internal code quality and test coverage improvements.

## [v0.1.0] - 2026-04-12

Initial release.
