# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
