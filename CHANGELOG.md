# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.3.0] - 2026-04-19

### Added

- `--aligned` — display output with whitespace-padded columns that line up on screen, independent of `--human-readable` (which formats individual values). Composes with `--no-sort`, `--header`, `--summarize`, and every `--show-*` flag; conflicts with `--json`. Works on both object and bucket listings.
- `-1` / `--one-line` — print just the key (or bucket name) per line, `ls -1`-style. All `--show-*` columns are ignored. With `--show-objects-only`, common prefixes are suppressed. With `--header`, a single `KEY`/`BUCKET` label is emitted. Conflicts with `--json`.
- `--target-no-sign-request` — read public (anonymous) S3 buckets without loading credentials. Requests are sent unsigned. Conflicts with `--target-profile`, `--target-access-key`, `--target-secret-access-key`, and `--target-session-token`.

### Changed

- Updated dependencies to latest compatible versions.
- Internal refactor of the display layer and expanded test coverage.

## [v0.2.0] - 2026-04-13

### Added

- Automated dependency auditing and code analysis via GitHub Actions.

### Fixed

- Fix incorrect documentation about why parallel listing is disabled for Express One Zone directory buckets.

### Changed

- Internal code quality and test coverage improvements.

## [v0.1.0] - 2026-04-12

Initial release.
