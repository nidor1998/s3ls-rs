# s3ls

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![codecov](https://codecov.io/gh/nidor1998/s3ls-rs/graph/badge.svg)](https://codecov.io/gh/nidor1998/s3ls-rs)

> **Note on issues:** This project continues to be maintained, and binaries will keep being released. However, to consolidate discussion across the [s3sync](https://github.com/nidor1998/s3sync) / [s3util-rs](https://github.com/nidor1998/s3util-rs) / [s3rm-rs](https://github.com/nidor1998/s3rm-rs) / [s3ls-rs](https://github.com/nidor1998/s3ls-rs) family, **please file new issues in the [s7cmd](https://github.com/nidor1998/s7cmd) repository** instead of here. [s7cmd](https://github.com/nidor1998/s7cmd) bundles these tools as subcommands built on the same underlying code, so its behavior matches the standalone binaries and it can be used in their place. **Before opening an issue, please read the Scope and Non-Goals sections in the READMEs of [s7cmd](https://github.com/nidor1998/s7cmd) and each project ([s3sync](https://github.com/nidor1998/s3sync) / [s3util-rs](https://github.com/nidor1998/s3util-rs) / [s3rm-rs](https://github.com/nidor1998/s3rm-rs) / [s3ls-rs](https://github.com/nidor1998/s3ls-rs))** — requests outside the documented scope will generally be declined. Existing issues in this repository will continue to be handled as usual.

## Fast Amazon S3 object listing tool

List S3 objects and buckets using parallel API calls. Built in Rust.

### Demo

This demo shows listing approximately 360,000 objects per second, listing 1,100,000 objects in 3 seconds.

![demo](media/demo.webp)

> *Benchmark: EC2 instance in the same region as the bucket. Results may vary depending on network conditions, bucket prefix distribution, and S3 endpoint proximity.*

## Table of contents

<details>
<summary>Click to expand to view table of contents</summary>

- [Overview](#overview)
    * [Why s3ls?](#why-s3ls)
    * [How it works](#how-it-works)
    * [Why it's fast](#why-its-fast)
    * [Why it's flexible](#why-its-flexible)
- [Features](#features)
    * [High performance](#high-performance)
    * [Powerful filtering](#powerful-filtering)
    * [S3 versioning](#s3-versioning)
    * [S3 Express One Zone support](#s3-express-one-zone-support)
    * [Flexible sorting](#flexible-sorting)
    * [Readable by both machines and humans](#readable-by-both-machines-and-humans)
    * [Low memory usage](#low-memory-usage)
    * [Observability](#observability)
    * [Easy to use](#easy-to-use)
    * [Flexibility](#flexibility)
- [Requirements](#requirements)
- [Installation](#installation)
    * [Pre-built binaries](#pre-built-binaries)
    * [Build from source](#build-from-source)
- [Usage](#usage)
    * [Trailing slash matters](#trailing-slash-matters)
    * [List objects](#list-objects)
    * [List objects recursively](#list-objects-recursively)
    * [Filter by regex](#filter-by-regex)
    * [Filter by size](#filter-by-size)
    * [Filter by modified time](#filter-by-modified-time)
    * [Filter by storage class](#filter-by-storage-class)
    * [Combined filters](#combined-filters)
    * [Sort results](#sort-results)
    * [Display options](#display-options)
    * [TSV output](#tsv-output)
    * [One-per-line output](#one-per-line-output)
    * [JSON output](#json-output)
    * [Version listing](#version-listing)
    * [Depth-limited recursive listing](#depth-limited-recursive-listing)
    * [Bucket listing](#bucket-listing)
    * [Custom endpoint](#custom-endpoint)
    * [Specify credentials](#specify-credentials)
    * [Anonymous access](#anonymous-access)
    * [Specify region](#specify-region)
- [Detailed information](#detailed-information)
    * [Parallel listing architecture](#parallel-listing-architecture)
    * [API request calculation](#api-request-calculation)
    * [Filtering order](#filtering-order)
    * [Sorting detail](#sorting-detail)
    * [Streaming mode](#streaming-mode)
    * [Versioning support detail](#versioning-support-detail)
    * [JSON output detail](#json-output-detail)
    * [Control character escaping detail](#control-character-escaping-detail)
    * [Bucket listing detail](#bucket-listing-detail)
    * [S3 Permissions](#s3-permissions)
    * [CLI process exit codes](#cli-process-exit-codes)
- [Advanced options](#advanced-options)
    * [--max-parallel-listings](#--max-parallel-listings)
    * [--max-parallel-listing-max-depth](#--max-parallel-listing-max-depth)
    * [--no-sort](#--no-sort)
    * [--tsv](#--tsv)
    * [--max-keys](#--max-keys)
    * [--filter-include-regex/--filter-exclude-regex](#--filter-include-regex--filter-exclude-regex)
    * [-v](#-v)
    * [--aws-sdk-tracing](#--aws-sdk-tracing)
    * [--auto-complete-shell](#--auto-complete-shell)
    * [--help](#--help)
- [All command line options](#all-command-line-options)
- [CI/CD Integration](#cicd-integration)
- [Shell completions](#shell-completions)
- [About testing](#about-testing)
- [Security assumptions](#security-assumptions)
- [Fully AI-generated (human-verified) software](#fully-ai-generated-human-verified-software)
- [Scope](#scope)
- [Non-Goals](#non-goals)
- [Contributing](#contributing)
- [License](#license)

</details>

## Overview

### Why s3ls?

The standard `aws s3 ls` command makes sequential `ListObjectsV2` API calls — one page at a time. When you have hundreds of thousands or millions of objects, this becomes painfully slow.

s3ls takes a fundamentally different approach by discovering virtual directories and listing them concurrently.

### How it works

s3ls uses a three-stage streaming pipeline connected by bounded async channels:

```
[Lister + Filter Chain] → channel → [Aggregator] → channel → [DisplayWriter] → stdout
                   ↑                               ↓                          ↓
         parallel prefix discovery          sort (or stream)          format + output
```

1. **Lister + Filter Chain** — Sends concurrent S3 API calls using parallel prefix discovery. Uses the S3 delimiter feature to discover "virtual directories" (common prefixes) at the top levels of the hierarchy, then lists each prefix independently and concurrently, with up to 64 parallel operations by default. A semaphore prevents overwhelming S3 while maximizing throughput. Filters (regex, time range, size range, storage class) are applied inline as entries arrive — objects that don't match are discarded immediately without being forwarded to the aggregator.
2. **Aggregator** — In default mode, buffers all entries and sorts them. In streaming mode (`--no-sort`), passes each entry through immediately with no buffering. Computes statistics when summary output is requested.
3. **DisplayWriter** — Receives sorted (or streamed) entries and formats them as whitespace-aligned text (default), TSV (`--tsv`), one-per-line keys (`-1`), or NDJSON (`--json`), writing the result to stdout.

### Why it's fast

The speed comes from the parallel listing architecture, not from the choice of programming language (Rust). The bottleneck when listing S3 objects is network round-trip latency — each `ListObjectsV2` call takes milliseconds to return, and with 1,000 objects per page, listing 200,000 objects sequentially requires 200 round-trips waiting one after another. s3ls eliminates this wait by discovering virtual directories and listing each one concurrently.

Rust contributes low per-object overhead (no garbage collector pauses, small struct sizes, zero-cost abstractions for the async runtime), but this is a secondary factor. The primary speedup is architectural.

### Why it's flexible

The pipeline stages are decoupled through channels and trait abstractions. Filters are composed as a chain within the lister. Sorting and display are separated — the aggregator handles ordering while the display writer handles aligned-text, TSV, one-per-line, and NDJSON formatting through a common `EntryFormatter` trait. Adding a new filter, sort field, or output column does not require changes to the pipeline coordination — each concern is isolated.

## Features

### High performance

s3ls lists approximately 360,000 objects per second through parallel S3 API calls (1.1M objects in 3 seconds on an EC2 instance).

- Up to 64 concurrent listing operations by default (configurable up to 65,535)
- Parallel prefix discovery at configurable depth
- Parallel sorting for large result sets (threshold: 1,000,000 objects)
- Tested with buckets containing over 1 million objects

Parallel listing relies on the S3 delimiter feature to discover common prefixes (virtual directories) and list each one concurrently. If a bucket stores a large number of objects without any prefix hierarchy (e.g., all keys are flat like `file1.txt`, `file2.txt`, ... with no `/` separators), there are no sub-prefixes to split work across, and listing falls back to sequential pagination.

### Powerful filtering

Multiple filter types can be combined with AND logic:

- **Regex include/exclude** — Filter object keys using regular expressions (`--filter-include-regex`, `--filter-exclude-regex`)
- **Modified time range** — Filter by modification time using RFC 3339 format. `--filter-mtime-before` matches strictly before the given time; `--filter-mtime-after` matches at or after the given time
- **Size range** — Filter by object size with human-readable suffixes (KB, KiB, MB, MiB, GB, GiB, TB, TiB). `--filter-smaller-size` matches objects strictly smaller than the threshold; `--filter-larger-size` matches objects greater than or equal to the threshold
- **Storage class** — Filter by storage class (`--storage-class STANDARD,GLACIER,DEEP_ARCHIVE`)

### S3 versioning

s3ls supports listing all object versions including delete markers:

- `--all-versions` — List all versions including delete markers
- `--hide-delete-markers` — Filter out delete markers from version listing
- `--show-is-latest` — Display which version is current

### S3 Express One Zone support

s3ls supports S3 Express One Zone directory buckets:

- `--list-express-one-zone-buckets` — List only Express One Zone directory buckets
- `--allow-parallel-listings-in-express-one-zone` — Enable parallel listing in Express One Zone buckets

Parallel listing is disabled by default for Express One Zone directory buckets due to a `ListObjectsV2` behavior specific to directory buckets:

**CommonPrefixes pollution** — When querying with a delimiter during in-progress multipart uploads, the `CommonPrefixes` response includes prefixes associated with those uploads. This can produce spurious prefixes that don't correspond to actual object key hierarchies, making prefix-based parallelization unreliable.

s3ls detects Express One Zone buckets by their `--x-s3` name suffix and falls back to sequential listing as a conservative default. Pass `--allow-parallel-listings-in-express-one-zone` to opt in.

To enable parallel listing on Express One Zone buckets:

```bash
s3ls --recursive --allow-parallel-listings-in-express-one-zone s3://my-bucket--usw2-az1--x-s3/
```

### Flexible sorting

- Multi-column sort with up to 2 fields (`--sort key,size`)
- Sort by key, size, or date for objects; bucket, region, or date for buckets
- Reverse sort order (`--reverse`)
- Disable sorting entirely for streaming mode (`--no-sort`)

### Readable by both machines and humans

s3ls is designed from the ground up so that every byte of output is useful to a human reading a terminal and to a program parsing a pipe — in both of its output formats.

**Whitespace-aligned text** (default) — Each line is a single record. Each non-KEY column is padded to a fixed width and rows are separated by two spaces, so columns line up visually in a terminal and the output is easy for a human to scan. Control characters in S3 keys (`\x00`-`\x1f`, `\x7f`) are escaped as `\xNN` hex by default, so a maliciously-named object cannot inject newlines or ANSI sequences into terminal output or break downstream line-oriented parsing. Use `--raw-output` to disable escaping when trusting bucket contents. Add `--header` for a labeled header row that makes wide output self-documenting without interfering with `tail -n +2` workflows.

For pipelines and Unix tooling, add `--tsv` to emit tab-separated
output instead. `cut`, `awk`, `sort`, and friends can then process
the output directly without custom delimiters or quoting rules. The
choice of layout is independent of `--human-readable` (which formats
individual values).

**NDJSON** (`--json`) — One JSON object per line. Field names use PascalCase (`Key`, `Size`, `LastModified`, `ETag`, `StorageClass`) matching the S3 API response structure exactly. This means `jq`, Python scripts, and any tooling that already parses S3 API responses can consume s3ls output with zero translation. Every field returned by S3 for a given object is included in its JSON record. A few fields require an explicit opt-in because they cost an extra request parameter or rely on data S3 omits by default — `--show-owner` enables `Owner` for `ListObjectsV2` (it is always present for `ListObjectVersions`), and `--show-restore-status` enables `RestoreStatus`; for bucket listing, `--show-bucket-arn` and `--show-owner` gate the corresponding fields. Beyond those, the `--show-*` flags affect only column selection in text output. Each line is independently parseable, making the output compatible with streaming processors, log aggregation systems, and `jq` filters alike. Humans can read individual records, and machines can process millions of them without loading the entire output into memory.

Both formats share the same design principle: one record per line, stable field order, no surprises.

s3ls exposes S3 object metadata that other listing tools do not surface:

- **ETag** (`--show-etag`)
- **StorageClass** (`--show-storage-class`)
- **ChecksumAlgorithm** (`--show-checksum-algorithm`) — CRC32, CRC32C, SHA1, SHA256, CRC64NVME
- **ChecksumType** (`--show-checksum-type`) — FULL_OBJECT, COMPOSITE
- **Owner** (`--show-owner`) — DisplayName and ID
- **RestoreStatus** (`--show-restore-status`) — Restore progress and expiry for Glacier/Deep Archive objects
- **IsLatest** (`--show-is-latest`) — Version marker (requires `--all-versions`)
- **Bucket ARN** (`--show-bucket-arn`) — For bucket listing

### Low memory usage

By default, s3ls buffers all results in memory for sorting. Measured memory usage (RSS) on EC2 with `--max-parallel-listings 64`:

| Objects | Default (sorted) | `--no-sort` (streaming) |
|--------:|------------------:|------------------------:|
| 0 (baseline) | ~15 MB | ~15 MB |
| 100,000 | ~97 MB | — |
| 900,000 | ~543 MB | — |
| 1,100,000 | **~785 MB** | **~84 MB** |

In default sorted mode, each object consumes ~700-860 bytes of memory (struct + heap strings + allocator overhead), plus a ~15 MB baseline for the async runtime, AWS SDK, and connection pool.

In `--no-sort` streaming mode, memory stays at **~84 MB regardless of object count** — entries are written to stdout immediately and never buffered. This is 9x less memory than sorted mode for 1.1 million objects, and the gap grows linearly with object count.

If you still need sorted output for very large buckets, you can stream to a file and sort externally:

```bash
# Stream to a file, then sort by the 3rd column (key) using the OS sort command
s3ls --recursive --no-sort --tsv s3://huge-bucket/ > listing.tsv
sort -t$'\t' -k3 listing.tsv > listing_sorted.tsv
```

The OS `sort` command automatically spills to disk when the data exceeds available memory, so this approach works for any bucket size.

### Observability

s3ls provides structured logging through the `tracing` framework:

- `-v` / `-vv` / `-vvv` — Increase logging verbosity (info / debug / trace)
- `-q` / `-qq` — Decrease logging verbosity (error / silent)
- `--json-tracing` — Structured JSON log output for log aggregation systems
- `--aws-sdk-tracing` — Include AWS SDK internal traces
- `--span-events-tracing` — Include span open/close events
- `--disable-color-tracing` — Disable colored log output

### Easy to use

s3ls uses standard AWS credential mechanisms and requires no configuration files. It works out of the box with existing AWS CLI profiles, environment variables, and IAM roles.

```bash
# List all objects recursively
s3ls --recursive s3://my-bucket/

# List all your buckets
s3ls
```

### Flexibility

The following flags are available for connecting to S3-compatible storage services:

- Custom endpoints via `--target-endpoint-url` (MinIO, Wasabi, Cloudflare R2, etc.)
- Path-style access via `--target-force-path-style`
- S3 Transfer Acceleration via `--target-accelerate`
- Requester-pays via `--target-request-payer`
- Anonymous (unsigned) requests via `--target-no-sign-request` for public buckets
- HTTP/HTTPS proxy via standard environment variables (`HTTPS_PROXY`, `HTTP_PROXY`)

> **S3-compatible storage is not supported.** These flags remain available and may work, but their use against non-Amazon services is provided **as-is** — no testing, no support, no fixes for service-specific issues. See [About testing](#about-testing) for details.

s3ls is performance-tuned for Amazon S3, which supports high request rates. S3-compatible storage services may have lower rate limits. If you encounter throttling errors, use `--rate-limit-api` to cap the number of S3 API requests per second, or reduce concurrency with `--max-parallel-listings`:

```bash
# MinIO with rate limiting
s3ls --recursive \
     --target-endpoint-url http://localhost:9000 \
     --target-force-path-style \
     --rate-limit-api 50 \
     --max-parallel-listings 4 \
     s3://my-bucket/
```

## Requirements

- x86_64 Linux (kernel 3.2 or later)
- ARM64 Linux (kernel 4.1 or later)
- Windows 11 (x86_64, aarch64)
- macOS 11.0 or later (aarch64)

s3ls is distributed as a single binary with no dependencies (except glibc). Linux musl statically linked binary is also available.

AWS credentials are required for most buckets. s3ls supports all standard AWS credential mechanisms:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- AWS config file (`~/.aws/config`) with profiles
- IAM instance roles (EC2, ECS, Lambda)
- SSO/federated authentication

Public (anonymous) buckets can be read with `--target-no-sign-request` — no credentials are loaded and requests are sent unsigned.

For more information, see [SDK authentication with AWS](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html).

## Installation

### Pre-built binaries

Download a pre-built binary from [GitHub Releases](https://github.com/nidor1998/s3ls-rs/releases) for your platform:

| Platform | Binary |
|----------|--------|
| Linux x86_64 (glibc 2.28+) | `s3ls-*-linux-glibc2.28-x86_64.tar.gz` |
| Linux x86_64 (musl, static) | `s3ls-*-linux-musl-x86_64.tar.gz` |
| Linux aarch64 (glibc 2.28+) | `s3ls-*-linux-glibc2.28-aarch64.tar.gz` |
| Linux aarch64 (musl, static) | `s3ls-*-linux-musl-aarch64.tar.gz` |
| macOS Apple Silicon | `s3ls-*-macos-aarch64.tar.gz` |
| Windows x86_64 | `s3ls-*-windows-x86_64.tar.gz` |
| Windows ARM64 | `s3ls-*-windows-aarch64.tar.gz` |

### Build from source

```bash
cargo install --git https://github.com/nidor1998/s3ls-rs.git
```

## Usage

### Trailing slash matters

S3 is object storage, not a file system. There are no directories — only keys (strings) and prefixes (string matching). The prefix you specify is passed to the S3 API as a literal string match, and the presence or absence of a trailing slash changes which objects are returned.

```bash
# Without trailing slash: prefix = "data"
# Matches keys starting with "data" — including data/, data-backup/, database.txt
$ s3ls s3://my-bucket/data

# With trailing slash: prefix = "data/"
# Matches only keys starting with "data/" — the typical intended behavior
$ s3ls s3://my-bucket/data/
```

If you specify a prefix that does not exist, S3 simply returns an empty result and s3ls exits with code 0 (success). There is no "not found" error — in object storage, a prefix is not a resource that exists or doesn't exist, it is just a filter applied to key names.

This is not a quirk of s3ls — it is how the S3 `ListObjectsV2` API works. When in doubt, include the trailing slash to scope the listing to a specific "directory."

### List objects

```bash
# Non-recursive — shows objects and prefixes (PRE) at the current level
$ s3ls s3://my-bucket/data/
                                      PRE  data/2024/
                                      PRE  data/2025/
2024-01-15T10:30:00Z                 1234  data/readme.txt
```

### List objects recursively

```bash
$ s3ls --recursive s3://my-bucket/data/
2024-01-15T10:30:00Z                 1234  data/readme.txt
2024-06-01T08:00:00Z                 5678  data/2024/report.csv
2025-01-20T14:30:00Z                 9012  data/2025/summary.json
```

### Filter by regex

```bash
# Only .csv files
s3ls --recursive --filter-include-regex '\.csv$' s3://my-bucket/

# Exclude temporary files
s3ls --recursive --filter-exclude-regex '^tmp/' s3://my-bucket/
```

### Filter by size

```bash
# Files at least 100 MiB (≥)
s3ls --recursive --filter-larger-size 100MiB s3://my-bucket/

# Files strictly smaller than 1 KiB (<)
s3ls --recursive --filter-smaller-size 1KiB s3://my-bucket/
```

### Filter by modified time

```bash
# Files modified at or after a date (≥)
s3ls --recursive --filter-mtime-after 2025-01-01T00:00:00Z s3://my-bucket/

# Files modified strictly before a date (<)
s3ls --recursive --filter-mtime-before 2024-06-01T00:00:00Z s3://my-bucket/
```

### Filter by storage class

```bash
# Only GLACIER storage class
s3ls --recursive --storage-class GLACIER s3://my-bucket/

# Multiple storage classes
s3ls --recursive --storage-class STANDARD,GLACIER,DEEP_ARCHIVE s3://my-bucket/
```

### Combined filters

All filters use AND logic when combined:

```bash
s3ls --recursive \
  --filter-include-regex '\.parquet$' \
  --filter-larger-size 1GiB \
  --filter-mtime-after 2025-01-01T00:00:00Z \
  s3://my-bucket/data/
```

### Sort results

```bash
# Sort by size (largest first)
s3ls --recursive --sort size --reverse s3://my-bucket/

# Sort by date, then by key
s3ls --recursive --sort date,key s3://my-bucket/

# Stream results without sorting (lower memory usage for huge buckets)
s3ls --recursive --no-sort s3://my-bucket/
```

### Display options

```bash
# Human-readable sizes with summary
s3ls --recursive --human-readable --summarize s3://my-bucket/

# Show extra columns
s3ls --recursive --show-etag --show-storage-class s3://my-bucket/

# Add column headers
s3ls --recursive --header --show-storage-class s3://my-bucket/

# Show relative paths instead of full keys
s3ls --recursive --show-relative-path s3://my-bucket/data/
```

### TSV output

```
# Default — columns padded with spaces so the output scans well in a terminal
$ s3ls --recursive s3://my-bucket/data/
2024-01-15T10:30:00Z                 1234  data/readme.txt
2024-06-01T08:00:00Z                 5678  data/2024/report.csv

# TSV — machine-friendly, tab character between columns
$ s3ls --recursive --tsv s3://my-bucket/data/
2024-01-15T10:30:00Z	1234	data/readme.txt
2024-06-01T08:00:00Z	5678	data/2024/report.csv

# Default aligned, combined with --human-readable
$ s3ls --recursive --human-readable s3://my-bucket/data/
2024-01-15T10:30:00Z          1.2KiB  data/readme.txt
2024-06-01T08:00:00Z          5.5KiB  data/2024/report.csv
```

The default aligned layout pads each non-KEY column to a fixed width
so rows line up on screen. It is independent of `--human-readable`:

- `--human-readable` makes individual **values** human-friendly
  (e.g., `1.2KiB` rather than raw bytes).
- The default aligned layout makes the **layout** human-friendly
  (columns line up).

For `cut`, `awk`, and other Unix tools that prefer tab-separated
input, opt into `--tsv`. `--tsv` composes with `--no-sort`, `--header`,
`--summarize`, and every `--show-*` flag. It conflicts with `--json`
(NDJSON is not columnar).

### One-per-line output

```bash
# One key per line, no columns — good for piping into xargs, fzf, etc.
$ s3ls --recursive -1 s3://my-bucket/data/
data/readme.txt
data/2024/report.csv
data/2025/summary.json

# Long form is equivalent
$ s3ls --recursive --one s3://my-bucket/data/

# List bucket names only
$ s3ls -1
bucket-a
bucket-b

# Suppress common prefixes (emit only objects)
$ s3ls --recursive -1 --show-objects-only s3://my-bucket/data/

# With --header, a single "KEY" (or "BUCKET") label is emitted
$ s3ls --recursive --header -1 s3://my-bucket/data/
KEY
data/readme.txt
data/2024/report.csv
```

`-1` (long form `--one`) prints just the key (or bucket name)
per line, mimicking `ls -1`. All `--show-*` columns are ignored.
Common prefixes are included by default; add `--show-objects-only`
to drop them. Conflicts with `--json`.

### JSON output

```bash
# NDJSON output (one JSON object per line)
s3ls --recursive --json s3://my-bucket/

# Pipe to jq for further processing
s3ls --recursive --json s3://my-bucket/ | jq 'select(.Size > 1000000)'

# JSON output with summary
s3ls --recursive --json --summarize s3://my-bucket/
```

JSON output uses S3 API-aligned field names:

```json
{
  "Key": "test_files/dir_99/file_100000.txt",
  "LastModified": "2026-03-28T11:55:11Z",
  "ETag": "\"41895e43efae08f72b75dfcf35e3ed69\"",
  "ChecksumAlgorithm": ["CRC64NVME"],
  "ChecksumType": "FULL_OBJECT",
  "Size": 49,
  "StorageClass": "STANDARD",
  "Owner": {
    "ID": "b7673edd784a8e1e83b264bf4f3cce1bf277b9f6e7e6e5118d1c3bee880d406f"
  }
}
```

### Version listing

```bash
# List all object versions including delete markers
s3ls --recursive --all-versions s3://my-bucket/

# Show which version is latest
s3ls --recursive --all-versions --show-is-latest s3://my-bucket/

# Hide delete markers
s3ls --recursive --all-versions --hide-delete-markers s3://my-bucket/
```

### Depth-limited recursive listing

```bash
# Recursive but only 2 levels deep — shows PRE for deeper prefixes
s3ls --recursive --max-depth 2 s3://my-bucket/

# Useful for exploring bucket structure without listing everything
s3ls --recursive --max-depth 1 s3://my-bucket/data/
```

### Bucket listing

```bash
# List all buckets
s3ls

# Filter by name prefix
s3ls --bucket-name-prefix data

# Show bucket ARNs
s3ls --show-bucket-arn

# List Express One Zone directory buckets
s3ls --list-express-one-zone-buckets
```

### Custom endpoint

```bash
# MinIO
s3ls --target-endpoint-url http://localhost:9000 \
     --target-force-path-style \
     --target-access-key minioadmin \
     --target-secret-access-key minioadmin \
     s3://my-bucket/
```

### Specify credentials

```bash
# Use a named AWS profile
s3ls --target-profile production s3://my-bucket/

# Use explicit access keys
s3ls --target-access-key AKIAIOSFODNN7EXAMPLE \
     --target-secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
     s3://my-bucket/
```

### Anonymous access

Use `--target-no-sign-request` to read a public S3 bucket without
loading AWS credentials. Requests are sent unsigned (no SigV4), so no
access key, profile, or IMDS lookup is required.

```bash
# List a public bucket anonymously (replace with an actual public bucket)
s3ls --recursive --target-no-sign-request \
     --target-region us-east-1 \
     s3://example-public-bucket/

# Works with S3-compatible endpoints too
s3ls --recursive --target-no-sign-request \
     --target-endpoint-url https://s3.example.com \
     --target-force-path-style \
     s3://public-bucket/
```

`--target-no-sign-request` conflicts with `--target-profile`,
`--target-access-key`, `--target-secret-access-key`, and
`--target-session-token` — those flags imply a signing identity, so
mixing them with anonymous mode is ambiguous. `--target-region` is
still honored (and often required, since no profile is consulted to
supply a default region).

### Specify region

```bash
s3ls --target-region us-west-2 s3://my-bucket/
```

## Detailed information

### Parallel listing architecture

s3ls uses a two-phase architecture for recursive listing:

1. **Discovery phase** — Sends `ListObjectsV2` requests with a delimiter to discover common prefixes (virtual directories) at the top levels of the hierarchy, up to `--max-parallel-listing-max-depth` (default: 2).
2. **Listing phase** — Each discovered prefix is listed independently and concurrently. A semaphore limits the number of concurrent listing operations to `--max-parallel-listings` (default: 64).

Non-recursive listing always uses a single sequential listing operation.

**Limitation:** Parallel listing depends on discovering common prefixes (virtual directories separated by `/`). If a bucket contains a large number of objects stored without any prefix hierarchy — for example, all keys are flat like `file1.txt`, `file2.txt`, ... with no `/` separators — the discovery phase finds zero sub-prefixes, and the entire listing falls back to sequential pagination. The parallel infrastructure provides the most benefit on buckets with well-distributed prefix hierarchies.

### API request calculation

s3ls uses only `ListObjectsV2` (for current-version listing) and `ListObjectVersions` (for `--all-versions` listing). It does **not** call `HeadObject` or `GetObject` — all metadata displayed in the output (key, size, last modified, ETag, storage class, checksum, owner, restore status) comes from the list response itself.

s3ls sends one S3 API call per page of results. Each page returns up to `--max-keys` objects (default: 1,000, the S3 API maximum). The total number of API requests depends on the listing mode.

#### Sequential listing

Sequential listing is used for non-recursive listing, `--max-parallel-listings 1`, or when parallel listing falls back to sequential (flat key structures, Express One Zone).

```
API requests = ceil(total_objects / max_keys)
```

For example, 200,000 objects with the default `--max-keys 1000` requires 200 API requests.

#### Parallel listing

Parallel listing works in two phases. Both phases return objects — the distinction is how prefixes are handled, not whether objects are listed.

**Delimiter phase** (depth 0 to `--max-parallel-listing-max-depth`):

At each depth level, s3ls sends `ListObjectsV2` requests with `delimiter="/"`. Each response returns both objects at the current prefix level and `CommonPrefixes` for deeper levels. Objects are emitted immediately; sub-prefixes are queued for further exploration.

```
Requests per prefix = ceil((objects_at_level + prefixes_at_level) / max_keys)
Total delimiter requests = sum across all prefixes at all depths up to max parallel depth
```

**Non-delimiter phase** (beyond max parallel depth):

Each leaf prefix discovered in the delimiter phase is listed without a delimiter, returning all remaining objects under that prefix.

```
Requests per leaf prefix = ceil(objects_under_prefix / max_keys)
Total non-delimiter requests = sum across all leaf prefixes
```

**Total API requests = delimiter requests + non-delimiter requests**

Parallel listing may send more API requests than sequential listing because delimiter-based pages contain a mix of objects and prefixes (reducing effective objects-per-request), but the requests execute concurrently, which is why throughput is higher.

#### Version listing

When `--all-versions` is specified, s3ls uses `ListObjectVersions` instead of `ListObjectsV2`. The calculation is the same, but each version of an object (including delete markers) counts as a separate entry. A single object with 10 versions consumes 10 entries toward the `max_keys` page size.

#### Bucket listing

Bucket listing uses the `ListBuckets` API. S3 returns up to 1,000 buckets per page.

```
API requests = ceil(total_buckets / 1000)
```

#### Filters do not reduce API requests

The S3 `ListObjectsV2` API only supports server-side filtering by prefix. All other filters (`--filter-include-regex`, `--filter-exclude-regex`, `--filter-mtime-before`, `--filter-mtime-after`, `--filter-smaller-size`, `--filter-larger-size`, `--storage-class`) are applied client-side after the API response is received. This means filters reduce the number of entries in the output, but they do not reduce the number of API requests or the associated cost.

To reduce API requests, narrow the target prefix:

```bash
# Lists ALL objects in the bucket, then filters client-side — full API cost
s3ls --recursive --filter-include-regex '\.csv$' s3://my-bucket/

# Lists only objects under data/2025/ — fewer API requests
s3ls --recursive --filter-include-regex '\.csv$' s3://my-bucket/data/2025/
```

#### Estimating API requests with -v

Use `-vv` (debug logging) to see the total number of API calls at completion:

```
DEBUG Listing pipeline completed api_calls=3312
```

Use `-vvv` (trace logging) to see the actual S3 API calls with their parameters:

```
TRACE ListObjectsV2 request bucket=s3ls-rs-test prefix=Some("test_data_09/dir_89/") delimiter=Some("/") max_keys=1000 continuation_token=None
```

#### When to consider S3 Inventory instead

Every `ListObjectsV2` or `ListObjectVersions` API call incurs a cost. For a bucket with 10 million objects, a single full listing requires at least 10,000 API requests. If you need to enumerate the entire contents of a large bucket on a recurring basis (daily or weekly), [S3 Inventory](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html) may be more cost-effective. S3 Inventory generates a manifest of all objects in a bucket at a flat cost regardless of object count, delivered as CSV, ORC, or Parquet files to a destination bucket.

s3ls is suited for on-demand, interactive, or filtered listings where you need real-time results. S3 Inventory is suited for scheduled, full-bucket inventories where cost predictability matters more than immediacy.

### Filtering order

All filters are evaluated after each page of `ListObjectsV2` results is received. Filters are applied in the following order:

1. Include regex (`--filter-include-regex`)
2. Exclude regex (`--filter-exclude-regex`)
3. Modified time before (`--filter-mtime-before`)
4. Modified time after (`--filter-mtime-after`)
5. Smaller size (`--filter-smaller-size`)
6. Larger size (`--filter-larger-size`)
7. Storage class (`--storage-class`)

All filters use AND logic — an object must pass all specified filters to be included in the output.

### Sorting detail

s3ls supports multi-column sorting with up to 2 fields. Available sort fields differ by listing mode:

- **Object listing:** `key`, `size`, `date`
- **Bucket listing:** `bucket`, `region`, `date`

Default sort is `key` for objects and `bucket` for buckets. When using `--all-versions`, if exactly one sort field is specified and it is not `date`, `date` is automatically appended as a secondary sort so versions of the same key appear in chronological order. If two sort fields are already specified, no automatic append happens.

Sorting requires buffering all results in memory before output. For very large result sets, use `--no-sort` to stream results directly.

When the result set reaches `--parallel-sort-threshold` (default: 1,000,000), s3ls uses parallel sorting via the rayon library.

### Streaming mode

`--no-sort` disables sorting and streams results directly to stdout as they arrive from S3. This mode:

- Uses constant memory regardless of object count
- Outputs results in the order S3 returns them (lexicographic within each listing operation, but interleaved across parallel operations)
- Cannot be combined with `--sort` or `--reverse`

If sorted output is needed for a bucket too large to sort in memory, stream to a file and sort externally:

```bash
s3ls --recursive --no-sort --tsv s3://huge-bucket/ > listing.tsv
sort -t$'\t' -k3 listing.tsv > listing_sorted.tsv
```

The OS `sort` command automatically spills to disk when the data exceeds available memory.

### Versioning support detail

When `--all-versions` is specified, s3ls uses `ListObjectVersions` instead of `ListObjectsV2`. This returns all versions of each object, including delete markers. Each result includes a `VersionId` field.

- Delete markers are included by default but can be hidden with `--hide-delete-markers`
- The `--show-is-latest` flag adds a column showing whether each version is the current version
- In JSON output, `IsLatest` and `VersionId` fields are always included regardless of `--show-is-latest`

### JSON output detail

The `--json` flag outputs one JSON object per line (NDJSON format). Field names use PascalCase to match S3 API responses:

- `Key`, `Size`, `LastModified`, `ETag`, `StorageClass`, `ChecksumAlgorithm`, `ChecksumType`
- `Owner` (nested object with `DisplayName` and `ID`)
- `RestoreStatus` (nested object with `IsRestoreInProgress` and `RestoreExpiryDate`)
- `VersionId`, `IsLatest` (when using `--all-versions`)

JSON output includes every field S3 returns for a given object. Two flags also gate the underlying request and therefore JSON inclusion: `--show-owner` enables `fetch_owner=true` on `ListObjectsV2` (so `Owner` is omitted from object listings without it; `ListObjectVersions` always returns `Owner`), and `--show-restore-status` enables the `RestoreStatus` optional object attribute. For bucket listing, `--show-bucket-arn` and `--show-owner` likewise gate `BucketArn` and `Owner` in the JSON output. All other `--show-*` flags affect only column selection in text output.

### Control character escaping detail

In tab-delimited text output, s3ls escapes control characters in S3 keys (`\x00`-`\x1f` and `\x7f`) as `\xNN` hex notation. This prevents:

- Newline injection that could corrupt line-oriented output
- ANSI escape sequence injection that could manipulate terminal display
- Null byte injection

Use `--raw-output` to disable escaping when you trust the bucket contents and need byte-exact key output. JSON output always uses standard JSON string escaping regardless of `--raw-output`.

### Bucket listing detail

When no target is specified (or target is empty), s3ls lists all buckets accessible to the configured credentials. Bucket listing supports:

- `--bucket-name-prefix` — Filter buckets by name prefix
- `--list-express-one-zone-buckets` — List only S3 Express One Zone directory buckets
- `--show-bucket-arn` — Display bucket ARN
- `--show-owner` — Display bucket owner
- `--sort bucket` or `--sort region` — Sort by bucket name or region
- `--json` — NDJSON output with bucket metadata

### S3 Permissions

s3ls requires the following IAM permissions:

| Operation | Required Permission |
|-----------|-------------------|
| List objects | `s3:ListBucket` |
| List object versions | `s3:ListBucketVersions` |
| List buckets | `s3:ListAllMyBuckets` |
| Show owner | `s3:ListBucket` (with `fetch-owner` parameter) |
| Show restore status | `s3:ListBucket` (with `OptionalObjectAttributes` parameter) |
| Express One Zone buckets | `s3express:ListAllMyDirectoryBuckets` |

### CLI process exit codes

| Exit Code | Meaning |
|-----------|---------|
| 0 | Success (also returned when no objects match the given prefix) |
| 1 | General error |
| 2 | Invalid command line arguments |

## Advanced options

### --max-parallel-listings

Number of concurrent S3 API listing operations. Default: 64. Range: 1-65535.

Higher values increase throughput but also increase the number of concurrent S3 API calls. The default of 64 works well for most buckets. For buckets with very deep hierarchies, consider increasing this value.

```bash
s3ls --recursive --max-parallel-listings 64 s3://my-bucket/
```

### --max-parallel-listing-max-depth

Maximum depth at which s3ls discovers prefixes for parallel listing. Default: 2. Range: 1+.

A value of 2 means s3ls discovers prefixes up to 2 levels deep, then lists each discovered prefix sequentially. Increasing this value can improve throughput for buckets with deep hierarchies, but increases the discovery overhead.

```bash
s3ls --recursive --max-parallel-listing-max-depth 3 s3://my-bucket/
```

### --no-sort

Disables sorting and streams results directly to stdout. Reduces memory usage to near-zero for large buckets.

```bash
s3ls --recursive --no-sort s3://huge-bucket/
```

### --tsv

Switches the output from the default whitespace-aligned columns to
tab-separated text (TSV). TSV is machine-friendly: `cut`, `awk`,
`sort`, and other Unix tools can process it directly without custom
delimiters or quoting rules. Columns won't line up visually in a
terminal because tabs align to tab stops, not to content.

`--tsv` is independent of `--human-readable`:

- `--human-readable` changes individual **values** (`1.2KiB` instead
  of `1234`).
- `--tsv` changes the **layout** (tabs between columns instead of
  fixed-width spaces).

The two can be combined.

```bash
s3ls --recursive --tsv s3://my-bucket/data/
s3ls --recursive --tsv --human-readable --summarize s3://my-bucket/
```

**Zero buffering.** Both layouts stream rows one-by-one — neither
requires buffering the full result set. The default aligned layout
relies on bounded maximum widths derived from the S3 API contract
(e.g., a single object is at most 50 TiB = 14 digits, an ETag is at
most 38 chars), with the KEY column emitted last unpadded — so
aligned output composes cleanly with `--no-sort` (constant memory
for huge buckets) just like `--tsv`.

**Conflicts.** `--tsv` cannot be combined with `--json` (NDJSON
output is not columnar).

**Overflow (aligned mode only).** If a value exceeds its column width
(e.g., an unusually long VersionId, or an OwnerDisplayName with CJK
characters that render wider than the character count suggests), the
value is emitted as-is without truncation. Subsequent columns on that
row shift right, but no data is hidden. This mirrors `ls -l` behavior
for long filenames.

**`--raw-output` interaction.** Allowed in both layouts. With
`--raw-output`, control characters in keys or owner names are not
re-escaped, so in aligned mode column widths may be disrupted on rows
containing such bytes. This is a deliberate tradeoff implied by
`--raw-output` itself.

### --max-keys

Maximum number of objects returned per single `ListObjectsV2` API call. Default: 1000. Range: 1-1000.

Reducing this value may help with debugging or rate-limiting scenarios. The default of 1000 is the S3 API maximum and provides the best throughput.

```bash
s3ls --recursive --max-keys 100 s3://my-bucket/
```

### --filter-include-regex/--filter-exclude-regex

Regex patterns are matched against the full object key using the [fancy-regex](https://crates.io/crates/fancy-regex) engine, which supports lookaheads, lookbehinds, and other advanced features.

Include and exclude filters can be used together. Include is applied first — an object must match the include pattern AND not match the exclude pattern.

```bash
# Only .csv files, but not in the temp/ directory
s3ls --recursive --filter-include-regex '\.csv$' --filter-exclude-regex '^temp/' s3://my-bucket/
```

### -v

Increase logging verbosity. Can be specified multiple times:

- `-v` — Info level logging
- `-vv` — Debug level logging
- `-vvv` — Trace level logging (very verbose, includes per-page S3 API details)

```bash
s3ls -vv --recursive s3://my-bucket/
```

### --aws-sdk-tracing

Include AWS SDK internal traces in the logging output. Useful for debugging authentication, endpoint resolution, or retry behavior.

```bash
s3ls -v --aws-sdk-tracing --recursive s3://my-bucket/
```

### --auto-complete-shell

Generate shell completion scripts. Supported shells: bash, elvish, fish, powershell, zsh.

```bash
s3ls --auto-complete-shell bash > /etc/bash_completion.d/s3ls
s3ls --auto-complete-shell zsh > ~/.zfunc/_s3ls
s3ls --auto-complete-shell fish > ~/.config/fish/completions/s3ls.fish
```

### --help

Print help information. Use `--help` for full help including all options, or `-h` for a compact summary.

```bash
s3ls --help
```

## All command line options

<details>
<summary>Click to expand to view all command line options</summary>

```
$ s3ls -h
Fast S3 object listing tool

Usage: s3ls [OPTIONS] [TARGET]

Arguments:
  [TARGET]  S3 target path: s3://<BUCKET_NAME>[/prefix] (omit to list buckets) [env: TARGET=] [default: ""]

Options:
  -v, --verbose...  Increase logging verbosity
  -q, --quiet...    Decrease logging verbosity
  -h, --help        Print help (see more with '--help')
  -V, --version     Print version

General:
  -r, --recursive              List all objects recursively (enables parallel listing) [env: RECURSIVE=]
      --all-versions           List all versions including delete markers [env: LIST_ALL_VERSIONS=]
      --hide-delete-markers    Hide delete markers from version listing (requires --all-versions) [env: HIDE_DELETE_MARKERS=]
      --max-depth <MAX_DEPTH>  Maximum depth for recursive listing (requires --recursive) [env: MAX_DEPTH=]

Bucket Listing:
      --bucket-name-prefix <BUCKET_NAME_PREFIX>
          Filter buckets by name prefix (bucket listing only) [env: BUCKET_NAME_PREFIX=]
      --list-express-one-zone-buckets
          List only Express One Zone directory buckets (bucket listing only) [env: LIST_EXPRESS_ONE_ZONE_BUCKETS=]
      --show-bucket-arn
          Show bucket ARN column (bucket listing only) [env: SHOW_BUCKET_ARN=]

Filtering:
      --filter-include-regex <FILTER_INCLUDE_REGEX>
          List only objects whose key matches this regex [env: FILTER_INCLUDE_REGEX=]
      --filter-exclude-regex <FILTER_EXCLUDE_REGEX>
          Skip objects whose key matches this regex [env: FILTER_EXCLUDE_REGEX=]
      --filter-mtime-before <FILTER_MTIME_BEFORE>
          List only objects modified before this time [env: FILTER_MTIME_BEFORE=]
      --filter-mtime-after <FILTER_MTIME_AFTER>
          List only objects modified at or after this time [env: FILTER_MTIME_AFTER=]
      --filter-smaller-size <FILTER_SMALLER_SIZE>
          List only objects smaller than this size [env: FILTER_SMALLER_SIZE=]
      --filter-larger-size <FILTER_LARGER_SIZE>
          List only objects larger than or equal to this size [env: FILTER_LARGER_SIZE=]
      --storage-class <STORAGE_CLASS>
          Comma-separated list of storage classes to include [env: STORAGE_CLASS=]

Sort:
      --sort <SORT>  Sort results by field(s), comma-separated (default: key for objects, bucket for bucket listing) [env: SORT=] [possible values: key, size, date, bucket, region]
      --reverse      Reverse the sort order [env: REVERSE=]
      --no-sort      Disable sorting and stream results directly in arbitrary order (reduces memory usage) [env: NO_SORT=]

Display:
      --summarize                Append summary line (total count, total size) [env: SUMMARIZE=]
      --human-readable           Human-readable sizes (e.g. 1.2KiB) [env: HUMAN_READABLE=]
      --show-relative-path       Show key relative to prefix instead of full path [env: SHOW_RELATIVE_PATH=]
      --show-etag                Show ETAG column [env: SHOW_ETAG=]
      --show-storage-class       Show STORAGE_CLASS column [env: SHOW_STORAGE_CLASS=]
      --show-checksum-algorithm  Show CHECKSUM_ALGORITHM column [env: SHOW_CHECKSUM_ALGORITHM=]
      --show-checksum-type       Show CHECKSUM_TYPE column [env: SHOW_CHECKSUM_TYPE=]
      --show-is-latest           Show IS_LATEST column (requires --all-versions) [env: SHOW_IS_LATEST=]
      --show-owner               Show OWNER_DISPLAY_NAME and OWNER_ID columns [env: SHOW_OWNER=]
      --show-restore-status      Show IS_RESTORE_IN_PROGRESS and RESTORE_EXPIRY_DATE columns [env: SHOW_RESTORE_STATUS=]
      --show-local-time          Display timestamps in local time instead of UTC [env: SHOW_LOCAL_TIME=]
      --header                   Add a header row to each column [env: HEADER=]
      --json                     Output as NDJSON (one JSON object per line) [env: JSON=]
      --show-objects-only        Show only objects, hiding common prefixes (directory markers) from output [env: SHOW_OBJECTS_ONLY=]
      --raw-output               Emit raw S3 key/prefix bytes without escaping control characters [env: RAW_OUTPUT=]
      --tsv                      Emit tab-separated text (TSV) instead of the default whitespace-aligned columns [env: TSV=]
  -1, --one                      Display only the key (or bucket name), one per line, with no other columns. All `--show-*` options are ignored. For object listings, common prefixes are emitted unless `--show-objects-only` is set [env: ONE_LINE=]

Tracing/Logging:
      --json-tracing           Output structured logs in JSON format [env: JSON_TRACING=]
      --aws-sdk-tracing        Include AWS SDK internal traces in log output [env: AWS_SDK_TRACING=]
      --span-events-tracing    Include span open/close events in log output [env: SPAN_EVENTS_TRACING=]
      --disable-color-tracing  Disable colored output in logs [env: DISABLE_COLOR_TRACING=]

AWS Configuration:
      --aws-config-file <AWS_CONFIG_FILE>
          Path to the AWS config file [env: AWS_CONFIG_FILE=]
      --aws-shared-credentials-file <AWS_SHARED_CREDENTIALS_FILE>
          Path to the AWS shared credentials file [env: AWS_SHARED_CREDENTIALS_FILE=]
      --target-profile <TARGET_PROFILE>
          Target AWS CLI profile [env: TARGET_PROFILE=]
      --target-access-key <TARGET_ACCESS_KEY>
          Target access key [env: TARGET_ACCESS_KEY=]
      --target-secret-access-key <TARGET_SECRET_ACCESS_KEY>
          Target secret access key [env: TARGET_SECRET_ACCESS_KEY=]
      --target-session-token <TARGET_SESSION_TOKEN>
          Target session token [env: TARGET_SESSION_TOKEN=]
      --target-region <TARGET_REGION>
          AWS region for the target [env: TARGET_REGION=]
      --target-endpoint-url <TARGET_ENDPOINT_URL>
          Custom S3-compatible endpoint URL (e.g. MinIO, Wasabi) [env: TARGET_ENDPOINT_URL=]
      --target-force-path-style
          Use path-style access (required by some S3-compatible services) [env: TARGET_FORCE_PATH_STYLE=]
      --target-accelerate
          Enable S3 Transfer Acceleration [env: TARGET_ACCELERATE=]
      --target-request-payer
          Enable requester-pays for the target bucket [env: TARGET_REQUEST_PAYER=]
      --target-no-sign-request
          Do not sign the request. If this argument is specified, credentials will not be loaded [env: TARGET_NO_SIGN_REQUEST=]
      --disable-stalled-stream-protection
          Disable stalled stream protection [env: DISABLE_STALLED_STREAM_PROTECTION=]

Performance:
      --max-parallel-listings <MAX_PARALLEL_LISTINGS>
          Number of concurrent listing operations (1-65535) [env: MAX_PARALLEL_LISTINGS=] [default: 64]
      --max-parallel-listing-max-depth <MAX_PARALLEL_LISTING_MAX_DEPTH>
          Maximum depth for parallel listing operations [env: MAX_PARALLEL_LISTING_MAX_DEPTH=] [default: 2]
      --object-listing-queue-size <OBJECT_LISTING_QUEUE_SIZE>
          Internal queue size for object listing [env: OBJECT_LISTING_QUEUE_SIZE=] [default: 200000]
      --allow-parallel-listings-in-express-one-zone
          Allow parallel listings in Express One Zone storage [env: ALLOW_PARALLEL_LISTINGS_IN_EXPRESS_ONE_ZONE=]
      --rate-limit-api <RATE_LIMIT_API>
          Maximum S3 API requests per second for object listing operations [env: RATE_LIMIT_API=]
      --parallel-sort-threshold <PARALLEL_SORT_THRESHOLD>
          Minimum number of entries to trigger parallel sorting [env: PARALLEL_SORT_THRESHOLD=] [default: 1000000]

Retry Options:
      --aws-max-attempts <AWS_MAX_ATTEMPTS>
          Maximum retry attempts for AWS SDK operations [env: AWS_MAX_ATTEMPTS=] [default: 10]
      --initial-backoff-milliseconds <INITIAL_BACKOFF_MILLISECONDS>
          Initial backoff in milliseconds for retries [env: INITIAL_BACKOFF_MILLISECONDS=] [default: 100]

Timeout Options:
      --operation-timeout-milliseconds <OPERATION_TIMEOUT_MILLISECONDS>
          Overall operation timeout in milliseconds [env: OPERATION_TIMEOUT_MILLISECONDS=]
      --operation-attempt-timeout-milliseconds <OPERATION_ATTEMPT_TIMEOUT_MILLISECONDS>
          Per-attempt operation timeout in milliseconds [env: OPERATION_ATTEMPT_TIMEOUT_MILLISECONDS=]
      --connect-timeout-milliseconds <CONNECT_TIMEOUT_MILLISECONDS>
          Connection timeout in milliseconds [env: CONNECT_TIMEOUT_MILLISECONDS=]
      --read-timeout-milliseconds <READ_TIMEOUT_MILLISECONDS>
          Read timeout in milliseconds [env: READ_TIMEOUT_MILLISECONDS=]

Advanced:
      --max-keys <MAX_KEYS>
          Maximum number of objects returned in a single list object request (1-1000) [env: MAX_KEYS=] [default: 1000]
      --auto-complete-shell <AUTO_COMPLETE_SHELL>
          Generate shell completions for the given shell [env: AUTO_COMPLETE_SHELL=] [possible values: bash, elvish, fish, powershell, zsh]
```

</details>

## CI/CD Integration

s3ls can be used in automated pipelines for inventory, auditing, and monitoring.

### JSON logging

Enable structured JSON logs for log aggregation systems (Datadog, Splunk, CloudWatch, etc.):

```bash
s3ls --json-tracing --recursive s3://my-bucket/
```

### Quiet mode

Suppress log output for cleaner CI logs:

```bash
s3ls -qqq --recursive --json s3://my-bucket/ > inventory.jsonl
```

### Example GitHub Actions

```yaml
- name: Generate S3 inventory
  run: |
    s3ls --recursive --json s3://production-bucket/ > inventory.jsonl
    echo "Object count: $(wc -l < inventory.jsonl)"
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    AWS_DEFAULT_REGION: us-east-1
```

## Shell completions

```bash
# Generate completions for your shell
s3ls --auto-complete-shell bash > /etc/bash_completion.d/s3ls
s3ls --auto-complete-shell zsh > ~/.zfunc/_s3ls
s3ls --auto-complete-shell fish > ~/.config/fish/completions/s3ls.fish
```

## About testing

**Supported target: Amazon S3 only.**

S3-compatible storage is **not supported**. The custom-endpoint flags (`--target-endpoint-url`, `--target-force-path-style`, etc.) remain available and may work in practice, but any use against non-Amazon services is provided strictly **as-is** — no testing is performed, no support is offered, and bug reports or feature requests specific to S3-compatible storage will be closed without investigation.

s3ls has unit tests, property-based tests (proptest), and end-to-end integration tests, all run exclusively against Amazon S3. Since there is no official certification for S3-compatible storage, comprehensive testing across implementations is not possible.

## Security assumptions

s3ls is built on a fundamental security assumption: **both the object storage system and the specific bucket you list must be trusted.**

Within this trust model, s3ls implements the security measures you would reasonably expect of a listing tool: encrypted transport (TLS/HTTPS) for requests and the metadata they return, secure handling of credentials through the standard AWS credential providers (with access keys masked in logs and credential environment-variable values hidden from `--help` output), and control-character escaping of the object keys, prefixes, and owner names returned by S3 so that a maliciously-named object cannot inject terminal escape sequences or forge rows in the output. These measures protect your credentials, and the confidentiality and integrity of the listing metadata in transit, against transport-level and accidental threats.

Unlike a synchronization or transfer tool, s3ls never downloads object data — it reads only listing metadata, via `ListObjectsV2`, `ListObjectVersions`, and `ListBuckets`. It therefore performs no object-content integrity verification: the ETag and checksum columns it can display are values **reported by** the storage endpoint and surfaced for you to inspect, not guarantees that s3ls has independently verified.

s3ls also assumes that the storage endpoint is honest and non-adversarial — that it correctly implements the S3 list APIs and returns the object keys, metadata, and checksum values it actually stores, without tampering. The output is **not** a defense against a malicious or compromised storage backend that deliberately returns a fabricated listing, forged metadata, or falsified checksums. Against such an adversarial endpoint, these guarantees do not hold.

Crucially, trust must extend to the **bucket**, not just the storage provider. Even when the object storage system itself is fully trustworthy, a bucket can still be adversarial — for example, a bucket you do not control, a shared bucket writable by others, or one whose object names, metadata, or checksums were crafted by an attacker. If you list such a bucket, the keys and metadata it serves are already untrusted at the source, and s3ls's guarantees no longer apply. A trusted storage provider hosting an untrusted bucket is, for the purposes of this security model, an untrusted source. (s3ls escapes control characters in S3-returned strings by default precisely because object names are attacker-controlled — see [Control character escaping detail](#control-character-escaping-detail) — but that hardening is not a substitute for trusting the source.)

Listing an untrusted, compromised, or non-conformant endpoint or bucket is outside s3ls's security model. Selecting a trustworthy storage provider, and ensuring that every bucket you list is one you control or trust — including its credentials, encryption, and access policies — remains your responsibility.

## Fully AI-generated (human-verified) software

Every line of source code, every test, all documentation, CI/CD configuration, and this README were generated by AI using [Claude Code](https://docs.anthropic.com/en/docs/claude-code/overview) (Anthropic).

Human engineers authored the requirements, design specifications, and s3sync reference architecture. They thoroughly reviewed and verified the design, all source code, and all tests. All features of the initial build binary have been manually tested and verified by humans. All E2E test scenarios have been thoroughly verified by humans against live AWS S3. The development followed a spec-driven process: requirements and design documents were written first, and the AI generated code to match those specifications under continuous human oversight.

### Quality verification (by AI self-assessment)

Measurements below are taken at v1.2.0 (commit `568e981`, 2026-07-25). The coverage figures are sourced from `llvm-cov-report.txt` (`cargo llvm-cov`; `lcov.info` is the matching machine-readable LCOV artifact) and reflect a single combined run — `cargo llvm-cov` with `RUSTFLAGS="--cfg e2e_test"` on the maintainer's machine, 2026-07-25 — so both the unit tests and the live-AWS e2e suite are included in the report.

| Metric                         | Value                                                         |
|--------------------------------|---------------------------------------------------------------|
| Production code                | ~14,500 lines of Rust across 38 source files in `src/`        |
| Unit tests (in `src/`)         | 514 `#[test]` / `#[tokio::test]` annotations                  |
| E2E integration tests          | 113 annotations across 8 `tests/e2e_*.rs` files (gated behind `--cfg e2e_test`; run only by the maintainer against live AWS) |
| Code coverage (llvm-cov, combined unit + e2e run) | 97.51% regions (298 / 11,983 missed), 96.34% functions (28 / 764 missed), 98.31% lines (140 / 8,299 missed) |
| Static analysis (clippy)       | 0 warnings (`cargo clippy --all-features`)                    |
| Formatting                     | 0 diffs (`cargo fmt --all --check`)                           |
| Supply chain (cargo-deny)      | Clean (`cargo deny -L error check`); runs per-PR in `ci.yml` and daily at 01:34 UTC in `cargo-deny.yml`; `advisories.ignore = []` |
| Code adapted from [s3sync](https://github.com/nidor1998/s3sync) | Tracing infrastructure (`src/bin/s3ls/tracing_init.rs`) and the Ctrl+C signal handler (`src/bin/s3ls/ctrl_c_handler/`) |

What these numbers do and do not show:
- They show what the combined test run exercises — including the live-AWS e2e suite — not how the binary behaves under production load over time. CI asserts only the non-e2e build (unit tests) on every push and PR.
- Coverage is a structural metric. A covered line can still be incorrect; an uncovered line can still be correct. Use it to size the test surface, not to certify behaviour.
- The e2e suite covers live-AWS paths (recursive and versioned listings, filters, sorting, display flags, bucket listing, Express One Zone, a 16K-object listing-completeness run, and UTF-8/control-character edge cases) but runs only on the maintainer's machine; CI does not exercise it, and reproducing the coverage figures above requires AWS credentials.

The codebase is built through spec-driven development with human review at every step. Test counts and coverage will change as refinements are added.

### AI assessment of safety and correctness (by Claude, Anthropic)

<details>
<summary>Click to expand the full assessment</summary>

> Assessment date: 2026-07-25.
>
> Assessed version: 1.2.0 (commit `568e981`).
>
> Method and scope of evidence: this assessment was performed from scratch at v1.2.0, re-deriving every claim rather than carrying conclusions forward from other projects' assessments. All 38 Rust source files under `src/` (14,511 lines) were read in full, along with all 8 live-AWS suites in `tests/e2e_*.rs` (113 test annotations), `tests/common/mod.rs`, `Cargo.toml`, `Cargo.lock`, `deny.toml`, all five GitHub Actions workflows, and the coverage artifacts `lcov.info` / `llvm-cov-report.txt`. In addition, the following were executed on the assessed tree (rustc 1.97.1): `cargo test --all-targets` (514 passed — 506 library + 8 binary unit tests; the e2e crates compile as empty without `--cfg e2e_test`), `cargo fmt --all --check` (clean), `cargo clippy -- -D warnings` and `cargo clippy --all-features` (clean), and `cargo deny -L error check` (advisories, bans, licenses, sources all ok). Claims cite the file — and where useful the line numbers as of commit `568e981` — where the relevant code or test lives.
>
> Limits of the evidence: this assessment cannot rule out all bugs. It includes no fuzzing, sanitizer or Miri runs, formal verification, or penetration testing. The e2e suite runs only against the maintainer's AWS account; this pass did not run it (the coverage artifacts show its paths were executed during their generating run). Coverage measures what the tests execute, not whether the executed code is correct.

**Question addressed.** s3ls is a read-only listing tool — it cannot corrupt or delete S3 data. The failure modes that matter are therefore different from a transfer tool's: (1) a **silently incomplete or duplicated listing** that downstream automation treats as complete inventory, (2) **output injection** — attacker-named objects forging rows or terminal escapes in the operator's output, (3) **credential leakage**, (4) **runaway API usage or memory**, and (5) **misleading exit codes**. Each is examined below.

#### Surface

The binary has no subcommands and no mutating operation anywhere in `src/`: the only S3 calls in the codebase are `ListObjectsV2`, `ListObjectVersions`, `ListBuckets`, and `ListDirectoryBuckets` (`src/storage/s3/mod.rs:94, 176`; `src/bucket_lister.rs:295, 356`). Mutating SDK calls exist only in the test harness (`tests/common/mod.rs`) to build fixtures. Mode validation is strict and two-sided: bucket-listing mode rejects all 19 object-only flags and object mode rejects the 3 bucket-only flags with named error messages, rather than silently ignoring them (`src/config/args/mod.rs:710-787`); sort fields are validated per mode, capped at 2, and de-duplicated. Usage errors exit 2, listing errors exit 1, success (including an empty result) exits 0 — all three pinned by live-binary e2e tests (`tests/e2e_listing.rs:232-456`).

#### Listing completeness (the highest-stakes property)

- **Pagination anti-loop guards.** All six pagination loops — sequential objects/versions, parallel-discovery objects/versions, `ListBuckets`, `ListDirectoryBuckets` — fail loudly if S3 reports a truncated response without a forward marker, or returns the same continuation token/marker twice (`src/storage/s3/mod.rs:503-549, 635-679`; `src/bucket_lister.rs:332-345, 383-396`). A buggy or hostile S3-compatible endpoint can therefore truncate a listing but cannot make s3ls loop forever or silently skip pages that S3 said existed. Every guard has a dedicated unit test in both sequential and parallel form (`src/storage/s3/mod.rs:1502-1677, 2712-2919`).
- **Parallel engine correctness.** Recursive listings fan out by delimiter-discovered prefixes, which partition the keyspace — each object is returned by exactly one prefix scan, so parallelism cannot duplicate rows. The concurrency permit is held across leaf sequential scans (the v1.1.0 fix for unbounded leaf concurrency), released by parents before awaiting children so the semaphore cannot deadlock (`src/storage/s3/mod.rs:583-595`), and enforced by a regression test that measures peak in-flight requests against the mock fetcher (`mod.rs:2237-2304`). A sub-task error or panic cancels the whole pipeline and surfaces as exit 1 rather than a truncated exit-0 listing (`mod.rs:738-750`).
- **Pipeline shutdown discipline.** The lister drops its intermediate receiver before joining the storage task, so a cancelled run cannot deadlock against a full bounded queue — a fixed bug pinned by a timeout-guarded regression test (`src/lister.rs:70-74, 160-222`). Every spawned stage (storage, lister, aggregator, display writer) has its `JoinError` caught and converted into cancellation plus a reported error (`src/pipeline.rs:68-98`).
- **Filters fail closed.** `fancy-regex` matching returns `Result`, and a runtime error (e.g. backtrack-limit exhaustion on a pathological pattern) cancels the pipeline and exits 1 instead of silently keeping or dropping the entry — pinned for both regex filters with a forced `RuntimeError` (`src/filters/include_regex.rs:112-128`, `exclude_regex.rs:112-128`; `src/lister.rs:62-66`). Filter boundary semantics (`>=` for `--filter-larger-size` / `--filter-mtime-after`, strict `<` for their counterparts, `None` storage class ≡ `STANDARD`, delete markers exempt from size/class filters but subject to regex/mtime filters) are pinned by unit tests including `u64::MAX` edges and re-verified against live S3 with read-back `LastModified` pivots (`tests/e2e_filters.rs`, `e2e_filters_versioned.rs`).
- **Scale evidence.** The live suite includes a 16,082-object, 6–7-level-deep fixture verified for exact set-completeness under four configurations (full recursive, prefix-scoped, `--max-depth 3` with expected boundary prefixes, `--max-parallel-listing-max-depth 1`), plus 1,000-object paginated runs (`--max-keys 10`) in both objects and versions modes (`tests/e2e_large_listing.rs`, `e2e_listing.rs:636-732`).
- **Sorting.** Sort is stable (`sort_by` / rayon `par_sort_by`), multi-column via `Ordering::then_with`, with a test proving sequential and parallel sorts produce identical output and a live test proving the auto-appended secondary date sort for versioned listings (`src/aggregate.rs:144-168`; `tests/e2e_sort.rs:352-431`).

#### Output integrity (injection defense)

Object keys are attacker-controlled even on a trusted endpoint. By default, text-mode output replaces `\x00`–`\x1f` and `\x7f` with `\xNN` escapes in every string an attacker can influence — keys, common prefixes, owner display name/ID, bucket names — before padding or joining columns (`src/display/mod.rs:78-108`; `src/display/columns.rs`; `src/bucket_lister.rs:27-33`). The escape function is UTF-8-correct (byte fast-path is sound because UTF-8 continuation bytes are ≥ 0x80; slow path iterates by `char`), and a unit test pins the exact phantom-row attack — a key embedding `\n` + a fake TSV row — being neutralized (`src/display/tsv.rs:273-296`). Live tests upload keys containing literal tabs/newlines and 2–4-byte UTF-8 (emoji, CJK extension B) and assert both escaped text output and byte-exact JSON round-trips (`tests/e2e_edge_cases.rs`). JSON mode always emits via `serde_json` (control chars safely escaped, keys byte-preserved); `--raw-output` is an explicit opt-out and conflicts with `--json`. One asymmetry worth naming: `BucketRegion`, `BucketArn`, ETag, and version-ID values are *not* escaped in text mode — on Amazon S3 these are AWS-generated and cannot carry control bytes, but a hostile S3-compatible endpoint could inject via them. That is squarely outside the documented security model (see the README's Security assumptions), but operators of untrusted endpoints should know the escaping is scoped to attacker-controllable-on-AWS fields, not to every S3-returned string.

#### Credential handling

`AccessKeys` derives `Zeroize` + `ZeroizeOnDrop`; its `Debug` masks the access-key ID to first-4/last-4 (full redaction under 8 chars) and fully redacts the secret and session token — all pinned by unit tests (`src/types/mod.rs:45-90, 309-323`). The three secret-bearing arguments set `hide_env_values`, and a forked-process test proves `--help` shows the env var *names* but never their *values*, with a deliberate control (the non-secret `TARGET_REGION` value does appear, proving the assertion would catch a leak) (`src/config/args/tests.rs:1967-2006`). The `-vvv` config trace passes through the redacting `Debug` chain, so it cannot print secrets (`src/bin/s3ls/main.rs:37`). `--target-no-sign-request` disables credential loading entirely for public buckets (`client_builder.rs:120-123`). Residual, inherent exposure: secrets passed as command-line arguments are visible to local process inspection, and the SDK holds its own non-zeroizing copies — defense in depth, not complete erasure. `http://` custom endpoints are accepted (`value_parser/url.rs`) and remove transport confidentiality; that is an operator-controlled insecure mode.

#### Robustness against non-conforming endpoints

There are **no `unsafe` blocks in production code** (the only two `unsafe` sites are test-only env-var mutations under `rusty_fork`), and the production panic surface is six provably-infallible `expect`/`unwrap` sites (semaphore-closed, always-3-columns `pop`, `serde_json::to_string` on string/bool/int maps, re-parse of an already-validated regex and byte size). No S3 response field is unwrapped: a row missing its key or `LastModified` is *silently dropped* (`convert_object`, `src/storage/s3/mod.rs:913-916`), a missing `IsTruncated` is treated as end-of-listing, and a negative size clamps to 0. This is the fail-safe (no crash) rather than fail-loud choice; against a pathologically non-conforming endpoint it can under-report rather than error. Both release profiles set `panic = "abort"`, so any surviving panic path terminates the process without unwinding — relevant context for the guarantee above.

#### Operational semantics

- **Exit codes** (0/1/2) are pinned by live tests, including empty-bucket and no-matching-prefix → 0, `NoSuchBucket` and bad credentials → 1, and clap/validation errors → 2. `BrokenPipe` from `| head` exits 0 silently on both the object- and bucket-listing paths, pinned by shell-pipeline e2e tests (`src/bin/s3ls/main.rs:66-69, 94-97`; `tests/e2e_edge_cases.rs:704-787`).
- **Rate limiting** is exact: the schedule is property-tested for every accepted rate (10–65,535 rps, `refill × 1000 == rate × interval` — the v1.1.0 fix for flooring to multiples of 10), and the limiter's acquire is cancellation-aware (`src/storage/s3/mod.rs:878-894, 2148-2182`).
- **Memory**: the default sort mode buffers the entire result set, and the three bounded channels between stages (storage→lister, lister→aggregator, aggregator→display) each default to 200,000 entries (`src/config/mod.rs:91`; `src/pipeline.rs:57-59`; `src/lister.rs:26`); `--no-sort` streams with near-constant memory and still applies filters (live-pinned, `tests/e2e_filters.rs:1070-1104`). For very large buckets, memory is bounded only if the operator opts into `--no-sort`.

#### Supply chain and CI

The shipped TLS stack is `rustls 0.23.42` with `aws-lc-rs 1.17.3` and OS trust anchors; the legacy `rustls 0.21` alias is deliberately excluded (RUSTSEC-2026-0098; `Cargo.toml:31-37`), `ring` is not in the resolved dependency graph on the assessed platform, and `openssl-sys` is banned in `deny.toml`. `cargo deny -L error check` runs per-push/PR (`ci.yml`) and daily at 01:34 UTC (`cargo-deny.yml`) with `advisories.ignore = []`, unknown registries/git sources denied, and a permissive-license allowlist. `Cargo.lock` is committed; every release build and the crates.io publish use `--locked`; release artifacts get signed build-provenance attestations (`actions/attest-build-provenance@v4`) and publishing uses OIDC trusted publishing. CI builds and tests seven targets (x86_64/aarch64 × Linux gnu/musl, Windows MSVC ×2, macOS arm64) with blocking fmt, clippy `-D warnings`, and cargo-deny jobs. Gaps worth naming: no CI job compiles the `--cfg e2e_test` corpus (113 live tests are type-checked and run only on the maintainer's machine); the release workflow builds on tag push without running tests or waiting for `ci.yml`; most actions are referenced by mutable major-version tags; and CI's clippy does not pass `--all-targets` — this pass found two style-level lints (`format_in_format_args`, `needless_range_loop`) in unit-test code that the CI gate misses. Production code is clippy-clean under `-D warnings`.

#### Coverage measurement

`llvm-cov-report.txt` (with `lcov.info` as the same data in LCOV form, from a combined unit + live-e2e run — `src/bin/s3ls/main.rs` has zero embedded tests yet 100% function coverage, which is only possible with the spawned binary instrumented) reports 97.51% region, 96.34% function, and 98.31% line coverage overall. On the completeness-critical paths: `storage/s3/mod.rs` 97.84% regions / 98.60% lines, `lister.rs` 96.55%, `aggregate.rs` 99.08%, `pipeline.rs` 90.86%; the formatters `display/tsv.rs`, `aligned.rs`, `aligned_formatter.rs`, and `one_line_formatter.rs` are at 100.00% regions, `display/json.rs` 97.05%. The weakest files are `bucket_lister.rs` (91.60% regions, 82.22% functions) and `bin/s3ls/tracing_init.rs` (82.50%). Branch coverage is not measured (the branch columns are empty), and the artifacts come from a single maintainer-machine run; CI neither produces nor gates on coverage. Coverage is structural: a covered line can still be incorrect.

#### Known limitations and findings of this assessment

This from-scratch pass found no critical or high-severity defects at v1.2.0. What it did find, and the deliberate limits it confirmed:

- **Ctrl-C exits 0 with a truncated listing (main finding).** SIGINT cancels the pipeline, every stage treats cancellation as a graceful stop, and `S3lsError::Cancelled` maps to exit 0 (`src/types/error.rs:19-25`; `src/bin/s3ls/main.rs:100-103`). An interrupted `s3ls --recursive > inventory.txt` therefore produces a partial file and exit code 0 — indistinguishable from success for any supervisor that sends SIGINT and reads the exit status. Interactively this is arguably fine (the operator knows they interrupted); in automation it is a sharp edge. SIGTERM is not handled at all (default disposition kills the process, which at least cannot read as success). No test pins the SIGINT exit code.
- **Generic environment-variable names on every long option.** v1.2.0 removed the env binding from the positional target (an exported `TARGET` can no longer silently select a bucket — regression-tested in `src/config/args/tests.rs:2008-2025`), but every long option remains env-backed with generic names: an exported `RECURSIVE`, `JSON`, `SUMMARIZE`, `SORT`, `MAX_KEYS`, `HEADER`, etc. silently changes output shape or API-call volume. Command-line values win over the environment, and the blast radius for a read-only tool is output shape rather than data loss, but scripts parsing s3ls output in CI environments (where names like `JSON` are plausible) can be surprised.
- **Fail-safe, not fail-loud, on non-conforming responses.** Rows missing a key or timestamp are dropped without any warning, and a missing `IsTruncated` ends the listing as if complete. Correct-by-construction for Amazon S3 (these fields are always present); against a broken S3-compatible endpoint the result is silent under-reporting rather than an error. The anti-loop guards cover the inverse (loud failure on impossible pagination states), so the asymmetry is deliberate but worth knowing.
- `-qq` suppresses even error messages (tracing is never initialized) while the exit code stays correct — silent-by-design, but a failed run under `-qq` leaves no diagnostic.
- Sort-mode memory is unbounded in the object count; the documented mitigation is `--no-sort`.
- Minor documentation drift: `tests/README.md`'s per-file test counts and a few `Covers src/...:NNN` line references in e2e comments are stale relative to the current source. Cosmetic only.
- No fuzzing, sanitizers, Miri, or external audit; the e2e suite is maintainer-only. Fuzzing `escape_control_chars`, `S3Target::parse`, and the depth/prefix arithmetic (`key_depth` / `prefix_at_depth`) would be the highest-value next step.

The changelog fixes claimed for v1.1.0 and v1.2.0 were each verified present with a pinning regression test: the leaf-scan concurrency cap (`parallel_leaf_scans_respect_concurrency_limit`), the exact rate limiter (`rate_limiter_schedule_is_exact_for_all_valid_rates`), credential values hidden from `--help` (`help_hides_credential_env_values_but_shows_names`), and the positional `TARGET` env removal (`positional_target_ignores_target_env_var`).

#### Is the software reliable?

For readers who are not software engineers: "reliable" here means whether a careful operator can trust what `s3ls` prints — that the listing is complete, that it faithfully represents what S3 returned, and that running it cannot damage anything.

**Risk: damage to your data.** Structurally excluded: the binary contains only the four S3 list operations. There is nothing in the codebase that can write, modify, or delete an object or bucket.

**Risk: silently incomplete listings.** The engine fails loudly on impossible pagination states, propagates every worker error and panic into a non-zero exit, bounds concurrency correctly, and its completeness is verified against live S3 at 16,000-object scale in four configurations. The one gap is operator-shaped: an interrupted run (Ctrl-C) exits 0 with partial output, so automation must not treat an interruptible run's exit 0 as proof of completeness.

**Risk: misleading output.** Attacker-controlled object names cannot forge rows or inject terminal escapes in default text mode, and JSON mode is always safely escaped with byte-exact keys — both pinned by tests including real uploaded hostile keys.

**Risk: credential leakage.** Keys are zeroized and redacted in every debug/log/help path, with regression tests for the one leak that previously existed (`--help` printing exported credential values, fixed in v1.1.0).

**Risk: runaway cost.** Listing-only API surface, bounded parallelism (enforced by a regression test), an exact opt-in rate limiter, and `-v` API-call accounting.

**What this assessment cannot establish.** That the code is bug-free, that S3-compatible stores other than Amazon S3 will behave identically (the fail-safe conversions assume AWS-shaped responses), or that future dependency advisories will not bite before they are patched. In plain terms: at v1.2.0 the failure modes that could cause silent harm each have a specific, citable, tested safeguard, and the tool is reliable for routine use *provided* the operator treats exit codes — not output presence — as the success signal, avoids generic environment variables that collide with s3ls option names, and does not treat an interrupted run as a complete inventory. The fact that the codebase was generated by AI does not by itself raise or lower its reliability; what determines that is the design, the tests, and the verifiable evidence above.

</details>

### AI assessment of safety and correctness (by Codex)

<details>
<summary>Click to expand the full assessment</summary>

Assessment date: 2026-07-25.

Assessed workspace: manifest version 1.2.0, branch `fix/positional_envvar`, commit `9fce221`. The Rust source and build configuration are identical to commit `568e981`; the two later commits change the README and add the text coverage report.

This is a from-scratch assessment. All 38 Rust files under `src/` (14,511 lines, including embedded tests) and `build.rs` were read in full. The eight `tests/e2e_*.rs` suites, `tests/common/mod.rs`, manifests and resolved dependency graph, `deny.toml`, all five GitHub Actions workflows, and the latest `lcov.info` / `llvm-cov-report.txt` were inspected as supporting evidence. The pre-existing AI assessments in this README were not treated as evidence.

#### Verification performed

- `cargo test --all-targets` passed: 506 library tests and 8 binary tests (514 passed, 0 failed). The eight live-AWS e2e targets compiled with zero tests because they are gated by `cfg(e2e_test)`; this command therefore does not claim a live-AWS pass.
- `cargo fmt --all --check` passed.
- `cargo clippy --all-features -- -D warnings` passed for the normal library/binary targets. The stricter `cargo clippy --all-targets --all-features -- -D warnings` found two test-only style lints (`format_in_format_args` in `display/aligned.rs` and `needless_range_loop` in `display/tsv.rs`); production code remained clean.
- `cargo deny -L error check` passed: advisories, bans, licenses, and sources all reported `ok`, with no ignored advisories.
- The repository contains 514 test annotations under `src/` and 113 gated live-AWS e2e annotations across eight suites.
- Both coverage artifacts agree on 98.31% line coverage (8,159/8,299) and 96.34% function coverage (736/764). `llvm-cov-report.txt` additionally reports 97.51% region coverage (11,685/11,983).
- Branch coverage is not measured: both artifacts report zero branches. Coverage is execution evidence, not proof that assertions are sufficient or behavior is correct.
- Completeness-critical implementation coverage is high but not total: `storage/s3/mod.rs` is 98.60% lines, `lister.rs` 96.53%, `aggregate.rs` 99.07%, and `pipeline.rs` 92.67%. The weakest region coverage is `bin/s3ls/tracing_init.rs` at 82.50%, followed by `bin/s3ls/main.rs` at 90.72% and `bucket_lister.rs` at 91.60%.

#### Safety and correctness assessment

- The production S3 surface is read-only. The only SDK operations constructed under `src/` are `ListObjectsV2`, `ListObjectVersions`, `ListBuckets`, and `ListDirectoryBuckets`; there is no upload, copy, overwrite, or delete path. Object contents are never downloaded. s3ls therefore cannot corrupt S3 data, although incorrect or incomplete output can still mislead downstream automation.
- CLI validation separates bucket and object modes, rejects inapplicable flags and invalid sort fields, caps sort keys at two, rejects duplicates, and applies explicit 0/1/2 success/runtime/usage exit semantics. `--raw-output` is an explicit injection-safety opt-out and conflicts with JSON.
- All four object/version pagination paths (sequential objects, sequential versions, parallel discovery objects, parallel discovery versions) reject a truncated response without the required forward marker and reject a repeated token or key/version marker. The two bucket-listing loops reject repeated non-empty continuation tokens. These checks turn impossible Amazon S3 pagination states into errors instead of infinite loops.
- Under Amazon S3 delimiter semantics, recursive prefix discovery partitions the key namespace. Leaf scans retain a semaphore permit for their complete sequential pagination, parent scans release permits before joining children, and every child error or panic is surfaced. The bounded-channel shutdown order avoids the lister deadlock that can otherwise occur when cancellation leaves a producer blocked on a full queue.
- Filters compose with AND semantics. Regex runtime failures propagate and cancel the pipeline rather than silently accepting or discarding an entry. Size, mtime, storage-class, delete-marker, relative-path, maximum-depth, and stable multi-column sort boundaries have direct unit coverage.
- Default text output escapes C0 controls and DEL in keys, prefixes, bucket names, and owner fields before joining or padding columns. This blocks newline/tab row forgery and ANSI escape injection through attacker-controlled Amazon S3 names. JSON is serialized with `serde_json`, preserving values while escaping control characters syntactically. Other endpoint-generated fields such as ETag, version ID, region, ARN, checksum metadata, and restore dates are not passed through the text escape helper; Amazon S3 constrains those values, but an adversarial S3-compatible endpoint is outside this guarantee.
- Explicit access-key wrappers redact secrets in `Debug`, mask the access-key ID, and zeroize their owned strings on drop. Secret environment values are hidden from help, and anonymous mode disables credential loading. This is defense in depth rather than complete erasure: Clap initially owns ordinary `String` copies, the AWS SDK creates additional credential copies, command-line secrets can be exposed through shell history/process inspection, and `http://` custom endpoints deliberately remove transport confidentiality.
- Production Rust contains no `unsafe`; the four `unsafe` blocks are test-only environment mutations isolated in forked test processes. S3 response conversion does not unwrap optional fields. Instead, objects or versions missing a key or timestamp are silently omitted, absent `IsTruncated` is treated as false, and negative sizes clamp to zero. That is crash-resistant for malformed responses but can silently under-report against a non-conforming endpoint.
- Default sorted operation buffers the complete result set. `--no-sort` avoids that unbounded vector, but the three bounded pipeline channels each default to 200,000 entries, so its memory use is bounded rather than literally constant and can still be substantial for long keys/metadata.
- The rate limiter's sustained refill schedule is exact for every accepted rate and cancellation-aware while waiting for a token. It intentionally starts with a full `rate`-sized bucket, so it permits an initial burst; it is a sustained-rate control, not a strict no-burst quota.

#### Findings from this pass

**Functional configuration defect — command-line custom AWS file paths are ignored unless `--target-profile` is also selected.** `--aws-config-file` and `--aws-shared-credentials-file` are accepted independently and stored in `ClientConfigLocation`, but `build_profile_files()` is applied to credentials and region only inside the `S3Credentials::Profile` branches (`src/storage/s3/client_builder.rs:90-148`). In the default `FromEnvironment` mode, client creation returns to the SDK's ordinary default credential and region chains without applying either CLI path. A command such as `s3ls --aws-shared-credentials-file /isolated/credentials s3://bucket` can therefore use ambient/default credentials instead of the requested file, potentially listing the wrong account or failing authentication. Pairing the files with `--target-profile` works; setting the standard AWS file environment variables also remains visible to the SDK. Existing argument tests verify only that the paths are stored, while the client-construction tests using custom files always select a profile.

**Operational correctness defect — SIGINT can produce partial output with exit status 0.** The object-listing Ctrl-C handler cancels a token, and storage, lister, aggregator, and writer cancellation paths all return `Ok(())`; `main` consequently takes its success branch. The `S3lsError::Cancelled` variant also maps to 0 but is not constructed anywhere in production. Thus an interrupted `s3ls --recursive > inventory.txt` can leave a truncated inventory that automation checking only the exit status accepts as complete. No process-level test pins SIGINT status. Cancellation also does not wrap an in-flight AWS SDK `.send()`, so Ctrl-C may wait for the current request/retry/timeout before the token is observed. Bucket-listing mode installs no custom Ctrl-C handler and retains the operating system's default signal behavior.

No critical or high-severity vulnerability was identified in the complete reviewed Rust source. That is bounded by the verification limits below and is not a claim that none exists.

#### Residual risks and evidence limits

- The 16,082-object live-AWS test is strong evidence against omissions across full, prefix-scoped, depth-limited, and shallow-parallel configurations. Its parser collects output into a `HashSet`, however, so duplicate rows collapse and are not detected; it proves set completeness, not exactly-once multiplicity.
- Generic environment bindings (`RECURSIVE`, `JSON`, `SORT`, `MAX_KEYS`, `HEADER`, and similar names) can silently alter operation or output in a shell with colliding variables. The positional target is deliberately exempt, so `TARGET` cannot select a bucket.
- A trusted Amazon S3 response supplies required keys, timestamps, and pagination fields. A broken or hostile compatible endpoint can exploit the fail-safe conversion choices to cause silent omissions or inject controls through endpoint-generated text fields.
- Broken pipe is intentionally success so `s3ls ... | head` behaves like a Unix listing tool. Scripts requiring a complete inventory must not use an early-closing consumer.
- CI tests seven platform/target combinations, but does not compile or run the credentialed `cfg(e2e_test)` suite. The supplied coverage artifacts exercise real SDK listing paths, but are not a per-test pass log and are not regenerated or gated in CI.
- The release workflow builds on a tag without a test dependency, several actions use mutable major-version tags, the SARIF Clippy workflow is non-blocking, and blocking CI Clippy omits `--all-targets`.
- This pass did not run live AWS tests, fuzzers, Miri, sanitizers, fault injection, formal verification, penetration testing, or an independent audit.

#### Reliability conclusion

Overall classification: **conditionally reliable for routine Amazon S3 listing, with two operational/configuration defects that matter to automation**.

The strongest evidence is the read-only API surface, guarded object/version pagination, bounded parallel engine, fail-closed filter errors, structured task-error propagation, output-injection defenses, 514 passing local tests, and high combined coverage. No code path can damage an object or bucket.

Until the findings are fixed, automation should treat SIGINT as an indeterminate/incomplete run regardless of exit status, and custom AWS file flags should always be paired with an explicit `--target-profile` (or supplied through the standard AWS environment variables). Operators should also use HTTPS, least-privilege credentials, `--no-sort` for very large listings, and the default escaped or JSON output for buckets whose names are not fully trusted. These constraints are part of the reliability boundary; the software is well tested, not proven bug-free.

</details>

### AI assessment of safety and correctness (by Gemini)

<details>
<summary>Click to expand the full assessment</summary>

> Assessment date: 2026-07-25.
>
> Assessed version: 1.2.0 (commit `78db793`).
>
> Method and scope of evidence: this assessment was performed from scratch at v1.2.0, re-deriving all claims directly from source inspection and test execution. All 38 Rust source files under `src/` (14,511 lines) were read in full, along with all 8 live-AWS integration suites in `tests/e2e_*.rs` (113 test annotations), `tests/common/mod.rs`, `Cargo.toml`, `Cargo.lock`, `deny.toml`, all five GitHub Actions workflows, and the coverage artifacts `lcov.info` / `llvm-cov-report.txt`. In addition, the following were executed on the assessed tree (rustc 1.97.1): `cargo test --all-targets` (514 passed — 506 library + 8 binary unit tests; e2e integration targets compile as empty without `--cfg e2e_test`), `cargo fmt --all --check` (clean, 0 diffs), `cargo clippy --all-features -- -D warnings` (clean, 0 warnings; note `cargo clippy --all-targets` reports 2 style-level warnings in test modules), and `cargo deny check` (advisories, bans, licenses, sources all ok). Claims cite specific source files — and line numbers as of commit `78db793` — where the relevant code or test resides.
>
> Limits of the evidence: this assessment cannot rule out all potential software bugs. It includes no fuzzing, sanitizer or Miri execution runs, formal verification, or penetration testing. The live-AWS e2e test suite runs against maintainer AWS infrastructure; this assessment did not execute live AWS requests directly (the checked-in coverage artifacts document execution of those paths during their generating run). Coverage measures statement/region execution during testing, not logical correctness.

**Question addressed.** s3ls is a read-only listing tool — it contains no mutating S3 operations and cannot overwrite, corrupt, or delete data in S3. The critical failure modes evaluated are: (1) a **silently incomplete or duplicated listing** treated as complete inventory by downstream automation, (2) **output injection** (attacker-named objects forging rows or ANSI terminal escapes in operator output), (3) **credential leakage**, (4) **runaway API calls or memory consumption**, and (5) **misleading exit codes**.

#### Surface

The binary contains no subcommands and no mutating operation anywhere in `src/`: the only S3 calls in the codebase are `ListObjectsV2`, `ListObjectVersions`, `ListBuckets`, and `ListDirectoryBuckets` (`src/storage/s3/mod.rs:94, 176`; `src/bucket_lister.rs:295, 356`). Mutating SDK calls exist only in the test harness (`tests/common/mod.rs`) for fixture setup. CLI mode validation in clap strictly separates bucket-listing mode and object-listing mode: bucket mode rejects object-only flags and object mode rejects bucket-only flags with explicit error messages rather than silently ignoring them (`src/config/args/mod.rs:710-787`). Usage errors exit 2, listing errors exit 1, and successful execution (including empty listing results) exits 0 — all pinned by live-binary integration tests (`tests/e2e_listing.rs:232-456`).

#### Listing completeness (the highest-stakes property)

- **Pagination anti-loop guards.** All six pagination loops — sequential objects/versions, parallel-discovery objects/versions, `ListBuckets`, `ListDirectoryBuckets` — fail loudly with an error if S3 reports a truncated response without a forward continuation token/marker, or returns the exact same token/marker twice (`src/storage/s3/mod.rs:503-549, 635-679`; `src/bucket_lister.rs:332-345, 383-396`). A buggy or non-conforming endpoint can truncate a listing with an error, but cannot make s3ls loop infinitely or silently drop pages. Every pagination guard is covered by dedicated unit tests (`src/storage/s3/mod.rs:1502-1677, 2712-2919`).
- **Parallel engine correctness.** Recursive listings fan out across delimiter-discovered prefixes, cleanly partitioning the key namespace so each object is visited by exactly one prefix scan, preventing duplicate rows. Concurrency permits (`TokioSemaphore`) are held during leaf sequential listings and released before awaiting sub-task joins (`src/storage/s3/mod.rs:583-595`), eliminating semaphore deadlocks. Peak in-flight requests are verified against mock fetchers by regression tests (`mod.rs:2237-2304`). Sub-task panics or errors cancel the pipeline via `CancellationToken` and surface as exit 1 rather than a truncated exit 0 (`mod.rs:738-750`).
- **Pipeline shutdown discipline.** The lister stage drops its intermediate receiver channel before joining the storage task, ensuring a cancelled pipeline cannot deadlock against a full bounded channel (`src/lister.rs:70-74, 160-222`). Every spawned pipeline stage catches `JoinError` and converts it into pipeline cancellation plus an explicit error report (`src/pipeline.rs:68-98`).
- **Filters fail closed.** `fancy-regex` evaluation returns `Result`, and runtime matching errors (such as regex backtrack-limit exhaustion) cancel the pipeline and exit 1 instead of silently dropping or retaining entries (`src/filters/include_regex.rs:112-128`, `exclude_regex.rs:112-128`; `src/lister.rs:62-66`). Size and mtime boundary semantics (`>=` for `--filter-larger-size` / `--filter-mtime-after`, strict `<` for smaller/before filters, `None` storage class treated as `STANDARD`, delete markers exempt from size/class filters) are unit-tested with boundary values like `u64::MAX` and verified against live S3 (`tests/e2e_filters.rs`, `e2e_filters_versioned.rs`).
- **Scale evidence.** The live integration suite tests set completeness against a 16,082-object, 6–7 level deep fixture across four configurations (full recursive, prefix-scoped, `--max-depth 3`, `--max-parallel-listing-max-depth 1`), alongside 1,000-object paginated runs (`--max-keys 10`) in both object and version modes (`tests/e2e_large_listing.rs`, `e2e_listing.rs:636-732`).
- **Sorting.** Sorting is stable (`sort_by` / rayon `par_sort_by`), multi-column via `Ordering::then_with`, with unit tests verifying identical output between sequential and parallel sorts, and live tests proving automatic secondary date sort ordering for versioned listings (`src/aggregate.rs:144-168`; `tests/e2e_sort.rs:352-431`).

#### Output integrity (injection defense)

Object keys are user/attacker-controlled data on S3. In default text output modes (TSV, aligned, one-line), control characters (`\x00`–`\x1f` and `\x7f`) in attacker-influenced strings — object keys, common prefixes, owner names/IDs, bucket names — are sanitized into `\xNN` hex escapes before column alignment or formatting (`src/display/mod.rs:78-108`; `src/display/columns.rs`; `src/bucket_lister.rs:27-33`). The byte fast-path is sound for UTF-8 (continuation bytes are >= 0x80; slow path iterates by `char`), preserving multi-byte UTF-8 sequences. Unit tests confirm neutralization of phantom-row injection attacks (embedding `\n` and fake TSV rows in object keys) (`src/display/tsv.rs:273-296`). Live AWS tests upload keys with tabs, newlines, and 2–4-byte UTF-8 characters (emoji, CJK) to assert correct text escaping and byte-exact JSON serialization (`tests/e2e_edge_cases.rs`). JSON mode emits via `serde_json`, ensuring standard control-character escaping. `--raw-output` explicitly opts out of escaping and conflicts with `--json`. Note: system metadata fields (`BucketRegion`, `BucketArn`, ETag, Version ID) are not passed through text escape routines; while Amazon S3 guarantees these contain no control bytes, non-AWS S3 endpoints could theoretically inject control bytes through them.

#### Credential handling

`AccessKeys` derives `Zeroize` and `ZeroizeOnDrop` (`src/types/mod.rs:45-90, 309-323`). Its `Debug` implementation masks access key IDs to first 4 and last 4 characters (fully redacting IDs <= 8 chars) and completely redacts secret keys and session tokens. Secret-bearing CLI arguments enable `hide_env_values`, and process-forked tests verify `--help` prints environment variable names while suppressing secret values (`src/config/args/tests.rs:1967-2006`). Configuration trace logs (`-vvv`) format credential structs via redacting `Debug` implementations (`src/bin/s3ls/main.rs:37`). `--target-no-sign-request` skips credential resolution for public bucket access (`client_builder.rs:120-123`). Secrets supplied directly as CLI arguments remain visible in process table listings (`ps`), and custom `http://` URLs disable transport encryption by operator choice.

#### Robustness against non-conforming endpoints

There are **zero `unsafe` blocks in production code** (the only two `unsafe` sites in `tracing_init.rs` and `args/tests.rs` are test-only environment variable mutations under `rusty_fork`). The production panic surface consists of six provably infallible `expect`/`unwrap` sites (semaphore permit acquisition, fixed column layout indexing, `serde_json::to_string` on primitive maps, re-parsing of pre-validated regex patterns and byte sizes). No S3 response field is unwrapped: malformed rows missing key or timestamp fields are safely dropped (`convert_object`, `src/storage/s3/mod.rs:913-916`), missing `IsTruncated` defaults to false, and negative object sizes clamp to 0. This fail-safe design avoids crashes on malformed responses, though non-conforming third-party endpoints may result in dropped rows rather than loud errors. Release compilation profiles set `panic = "abort"`, ensuring any panic terminates the process without unwinding.

#### Operational semantics

- **Exit codes** (0, 1, 2) are pinned by integration tests: empty bucket or empty prefix matches return exit 0; S3 access errors and `NoSuchBucket` return exit 1; clap validation errors return exit 2. `BrokenPipe` from downstream pipeline tools (e.g. `| head`) exits 0 silently (`src/bin/s3ls/main.rs:66-69, 94-97`; `tests/e2e_edge_cases.rs:704-787`).
- **Rate limiting** is exact: leaky-bucket token schedules are property-tested across all valid rates (10–65,535 rps), and limiter permit acquisition handles cancellation signals cleanly (`src/storage/s3/mod.rs:878-894, 2148-2182`).
- **Memory usage:** Default sort mode buffers listing results in memory prior to sorting, with bounded intermediate channels (200,000 entries per stage) (`src/config/mod.rs:91`; `src/pipeline.rs:57-59`; `src/lister.rs:26`). Streaming mode (`--no-sort`) operates in near-constant memory while retaining full filtering capabilities (`tests/e2e_filters.rs:1070-1104`).

#### Supply chain and CI

The shipping TLS stack relies on `rustls 0.23.42` with `aws-lc-rs 1.17.3` and system trust anchors; the legacy `rustls 0.21` dependency is explicitly excluded to avoid RUSTSEC-2026-0098 (`Cargo.toml:31-37`), and `openssl-sys` is banned in `deny.toml`. `cargo deny check` runs on pull requests (`ci.yml`) and daily schedules (`cargo-deny.yml`) with zero ignored advisories. `Cargo.lock` is committed; release builds enforce `--locked` and emit OIDC build-provenance attestations (`actions/attest-build-provenance@v4`). CI tests 7 target architectures. Note: CI clippy checks omit `--all-targets`, missing 2 minor style-level warnings in test modules (`src/display/aligned.rs:154`, `src/display/tsv.rs:539`). Production code is clippy-clean under `-D warnings`.

#### Coverage measurement

`llvm-cov-report.txt` (and `lcov.info`) from combined unit + live-AWS e2e runs reports 97.51% region coverage (298/11,983 missed), 96.34% function coverage (28/764 missed), and 98.31% line coverage (140/8,299 missed). Critical paths exhibit high coverage: `storage/s3/mod.rs` (97.84% region / 98.60% line), `lister.rs` (96.55% region), `aggregate.rs` (99.08% region), and output formatters (`display/tsv.rs`, `aligned.rs`, `aligned_formatter.rs`, `one_line_formatter.rs`) at 100.00% region coverage. Weakest files are `bucket_lister.rs` (91.60% region, 82.22% function) and `tracing_init.rs` (82.50% region).

#### Known limitations and findings of this assessment

This from-scratch assessment identified no critical or high-severity security vulnerabilities at v1.2.0. Findings and design trade-offs confirmed:

- **Ctrl-C (SIGINT) exits 0 with partial output (main finding).** SIGINT cancels the pipeline, and `S3lsError::Cancelled` maps to exit code 0 (`src/types/error.rs:19-25`; `src/bin/s3ls/main.rs:100-103`). An interrupted `s3ls --recursive > listing.txt` produces a partial output file and exit status 0, making it indistinguishable from successful completion for automated scripts checking exit codes. SIGTERM is unhandled (relying on default OS termination).
- **Generic environment variable bindings on CLI options.** All long flags bind to generic env var names (`RECURSIVE`, `JSON`, `SORT`, `MAX_KEYS`, `HEADER`, etc.), meaning ambient environment variables can alter output formats or API call counts unless explicitly overridden on the command line.
- **Fail-safe dropping of non-conforming S3 fields.** Missing object keys or timestamps are dropped silently without errors, prioritizing process stability over loud failure on malformed non-AWS endpoints.
- `-qq` verbosity suppresses all error logging while preserving exit codes.
- Sorting mode memory scales with total object count; streaming mode (`--no-sort`) is the recommended mitigation for massive buckets.
- CI clippy check does not run with `--all-targets`, leaving 2 style warnings in test files unflagged in CI.

#### Is the software reliable?

For non-technical readers: "reliable" indicates whether an operator can rely on `s3ls` output — that listings are complete, correctly formatted, safe against credential leakage, and incapable of causing data damage.

**Risk: data corruption or loss.** Structurally impossible. The binary contains only read-only S3 list operations. It has no code paths capable of writing, modifying, or deleting objects or buckets.

**Risk: silently incomplete listings.** Safeguarded against pagination loops, worker panics, and filter failures, with set completeness verified against live S3 at 16,000-object scale. The single operational caveat is that Ctrl-C interrupts return exit status 0, so automation must not rely solely on exit code 0 if signals can interrupt execution.

**Risk: misleading output / terminal injection.** Attacker-controlled object names cannot inject row delimiters or terminal control sequences in default text modes, and JSON output is safely escaped.

**Risk: credential leakage.** Credentials are zeroized on drop and redacted in all logging, debug, and help displays.

**Risk: runaway API costs.** Bounded parallelism, exact rate-limiting options, and API request counter logs (`-v`).

**What this assessment cannot establish.** That the software is completely free of bugs, that non-AWS S3 endpoints behave identically in fail-safe edge cases, or that future dependencies will remain vulnerability-free. In conclusion, at v1.2.0 all major failure modes have tested, verifiable safeguards, and `s3ls` is reliable for production listing tasks provided operators manage generic environment variable risks and do not treat user-interrupted runs as complete inventories.

</details>

## Scope

s3ls is a listing-only tool. It is **not** intended to be a drop-in replacement for, or behaviorally compatible with, any other S3 client — examples include the AWS CLI (`aws s3`, `aws s3api`), `s5cmd`, `s3cmd`, `rclone`, `mc`, etc., but the same applies to any S3 listing or transfer tool. Its command-line flags, output columns, sort/filter semantics, and exit codes are designed around fast parallel listing and stable machine-readable output — not interoperability with another tool's interface. Output formats and flag names will not be adjusted to match any external tool, and scripts written against another S3 client should not be expected to work with s3ls unmodified. If you need full S3 functionality (copy, sync, presign, multipart upload, etc.) or compatibility with a specific tool's flag set, use that tool.

## Non-Goals

The following are explicitly out of scope and will not be added, regardless of demand:

- Object or bucket modification (copy, sync, move, delete, presign, multipart upload, tagging, policy, etc.). s3ls is read-only; for transfers use [s3sync](https://github.com/nidor1998/s3sync) or [s3util](https://github.com/nidor1998/s3util-rs), and for general S3 operations use the [s7cmd](https://github.com/nidor1998/s7cmd).
- Per-object `HeadObject` or `GetObject` calls. All metadata in the output comes from the list response itself — s3ls will not issue a second request to enrich a single row.
- APIs other than `ListObjectsV2`, `ListObjectVersions`, and `ListBuckets`. s3ls intentionally restricts itself to these three list APIs; `ListMultipartUploads`, `HeadObject`, `GetObject`, and others are out of scope.
- Glob or wildcard expansion in S3 prefixes. The prefix you specify is passed to S3 as a literal string match. For pattern-based matching, use `--filter-include-regex` / `--filter-exclude-regex`, evaluated client-side after listing.
- Compatibility with other S3 clients — neither in flag names and behavior, nor in feature coverage. The presence of a feature, flag, or output format in `aws s3`, `aws s3api`, `s5cmd`, `s3cmd`, `rclone`, `mc`, or any other S3 tool is not, by itself, a reason to add or change it in s3ls. Each request is evaluated only against s3ls's own scope and design principles. Use that other tool if you need its specific surface.
- A plugin or extension mechanism.

Issues and pull requests requesting any of the above will be closed.

## Contributing

- Bug reports are welcome, but responses are not guaranteed.
- Since this project is considered functionally complete, I will not accept any feature requests.
- If you find this project useful, feel free to fork and modify it as you wish.

🔒 I consider this project “complete” and will maintain it only minimally going forward.
However, I intend to keep the AWS SDK for Rust and other dependencies up to date monthly.

**Issue and PR lifecycle**

To keep the tracker focused, an issue or PR with no activity for 30 days is labeled `stale` and closed 7 days later unless a new comment (or, for PRs, a new commit) is added. Items labeled `pinned` or `security` are exempt; PRs are also exempt from `pinned`. Closed items can always be reopened.

## License

This project is licensed under the Apache-2.0 License.
