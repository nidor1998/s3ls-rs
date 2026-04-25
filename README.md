# s3ls

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![GitHub](https://img.shields.io/github/downloads/nidor1998/s3ls-rs/total?label=downloads%20%28GitHub%29)](https://github.com/nidor1998/s3ls-rs/releases)
[![codecov](https://codecov.io/gh/nidor1998/s3ls-rs/graph/badge.svg)](https://codecov.io/gh/nidor1998/s3ls-rs)

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
    * [Scope](#scope)
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
- [Fully AI-generated (human-verified) software](#fully-ai-generated-human-verified-software)
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

### Scope

s3ls is a listing-only tool. It is **not** intended to be a drop-in replacement for, or behaviorally compatible with, any other S3 client — examples include the AWS CLI (`aws s3 ls`) and `s5cmd`, but the same applies to any S3 listing or transfer tool. Its command-line flags, output columns, sort/filter semantics, and exit codes are designed around fast parallel listing and stable machine-readable output — not interoperability with another tool's interface. Output formats and flag names will not be adjusted to match any external tool, and scripts written against another S3 client should not be expected to work with s3ls unmodified. If you need full S3 functionality (copy, sync, presign, multipart upload, etc.) or compatibility with a specific tool's flag set, use that tool.

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
- **ChecksumType** (`--show-checksum-type`) — FULL_OBJECT, TRAILER
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

s3ls works with any S3-compatible storage service:

- Custom endpoints via `--target-endpoint-url` (MinIO, Wasabi, Cloudflare R2, etc.)
- Path-style access via `--target-force-path-style`
- S3 Transfer Acceleration via `--target-accelerate`
- Requester-pays via `--target-request-payer`
- Anonymous (unsigned) requests via `--target-no-sign-request` for public buckets
- HTTP/HTTPS proxy via standard environment variables (`HTTPS_PROXY`, `HTTP_PROXY`)

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
                                 	PRE	data/2024/
                                 	PRE	data/2025/
2024-01-15T10:30:00Z	1234	data/readme.txt
```

### List objects recursively

```bash
$ s3ls --recursive s3://my-bucket/data/
2024-01-15T10:30:00Z	1234	data/readme.txt
2024-06-01T08:00:00Z	5678	data/2024/report.csv
2025-01-20T14:30:00Z	9012	data/2025/summary.json
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
  "LastModified": "2026-03-28T11:55:11+00:00",
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
      --tsv                      Emit tab-separated text instead of the default whitespace-aligned columns [env: TSV=]
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

Support for S3-compatible storage is on a best-effort basis and may behave differently.
s3ls has been tested with Amazon S3. s3ls has unit tests, property-based tests (proptest), and end-to-end integration tests.

S3-compatible storage is not tested when a new version is released. Since there is no official certification for S3-compatible storage, comprehensive testing is not possible.

## Fully AI-generated (human-verified) software

No human wrote a single line of source code in this project. Every line of source code, every test, all documentation, CI/CD configuration, and this README were generated by AI using [Claude Code](https://docs.anthropic.com/en/docs/claude-code/overview) (Anthropic).

Human engineers authored the requirements, design specifications, and s3sync reference architecture. They thoroughly reviewed and verified the design, all source code, and all tests. All features of the initial build binary have been manually tested and verified by humans. All E2E test scenarios have been thoroughly verified by humans against live AWS S3. The development followed a spec-driven process: requirements and design documents were written first, and the AI generated code to match those specifications under continuous human oversight.

## License

This project is licensed under the Apache-2.0 License.
