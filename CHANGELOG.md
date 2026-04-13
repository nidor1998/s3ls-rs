# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.2.0] - 2026-04-13

### Changed

- Flatten `S3Object` from a two-variant enum (`NotVersioning`/`Versioning`) into a single struct with `Option<VersionInfo>`, reducing code duplication across 19 files.
- Apply the same `VersionInfo` pattern to `ListEntry::DeleteMarker` for consistency.
- Split the monolithic aggregator into a three-stage pipeline with `EntryFormatter` polymorphism (`FormatterMessage` enum, `FormatterConfig` struct).

### Fixed

- Correct the Express One Zone parallel listing explanation in README: the actual limitations are the prefix-must-end-in-delimiter restriction and `CommonPrefixes` pollution from in-progress multipart uploads, not single-AZ rate limits.
- Rewrite parallel listing API request calculation to clarify that both phases (delimiter and non-delimiter) return objects.

### Added

- Comprehensive unit and e2e tests to improve coverage.
- GitHub Actions workflows for daily `cargo-deny` license/advisory checks and `rust-clippy` SARIF analysis.
- Dependabot configuration for monthly Cargo dependency updates.
- Pull request template directing external contributors to open issues.

## [v0.1.0] - 2026-04-12

Initial release.
