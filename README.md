# s3ls

Fast S3 object listing tool written in Rust.

**200,000 objects listed in 1.4 seconds** — that's ~145,000 objects/sec, achieved through massively parallel S3 API calls.

```
$ time s3ls --recursive s3://data.cpp17.org | wc -l
  200002

real    1.38s
```

For comparison, `aws s3 ls --recursive` typically processes around 5,000-8,000 objects/sec on the same bucket. s3ls is **~20x faster**.

## Why s3ls?

The standard `aws s3 ls` command makes sequential `ListObjectsV2` API calls — one page at a time. When you have hundreds of thousands or millions of objects, this becomes painfully slow.

s3ls takes a fundamentally different approach:

1. **Parallel prefix discovery** — It uses the S3 delimiter feature to discover "virtual directories" (common prefixes) at the top levels of your bucket hierarchy.
2. **Concurrent listing** — Each discovered prefix is listed independently and concurrently, with up to 32 parallel listing operations by default.
3. **Semaphore-gated parallelism** — A configurable semaphore prevents overwhelming S3 while maximizing throughput.

This architecture means s3ls gets faster on buckets with well-distributed prefix hierarchies — exactly the kind of buckets that are slowest with sequential tools.

## Installation

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

Or build from source:

```bash
cargo install --git https://github.com/nidor1998/s3ls-rs.git
```

## Quick Start

```bash
# List objects in a bucket (non-recursive, like `ls`)
s3ls s3://my-bucket/

# List all objects recursively
s3ls --recursive s3://my-bucket/

# List with human-readable sizes and summary
s3ls --recursive --human-readable --summarize s3://my-bucket/

# List all your buckets
s3ls
```

## Usage Examples

### Basic Listing

```bash
# Non-recursive — shows objects and prefixes (PRE) at the current level
$ s3ls s3://my-bucket/data/
                                 	PRE	data/2024/
                                 	PRE	data/2025/
2024-01-15T10:30:00Z	1234	data/readme.txt

# Recursive — all objects under a prefix
$ s3ls --recursive s3://my-bucket/data/
2024-01-15T10:30:00Z	1234	data/readme.txt
2024-06-01T08:00:00Z	5678	data/2024/report.csv
2025-01-20T14:30:00Z	9012	data/2025/summary.json
```

### Filtering

```bash
# Only .csv files
s3ls --recursive --filter-include-regex '\.csv$' s3://my-bucket/

# Exclude temporary files
s3ls --recursive --filter-exclude-regex '^tmp/' s3://my-bucket/

# Files modified after a date
s3ls --recursive --filter-mtime-after 2025-01-01T00:00:00Z s3://my-bucket/

# Files larger than 100MB
s3ls --recursive --filter-larger-size 100MiB s3://my-bucket/

# Only GLACIER storage class
s3ls --recursive --storage-class GLACIER s3://my-bucket/

# Combine multiple filters (AND logic)
s3ls --recursive \
  --filter-include-regex '\.parquet$' \
  --filter-larger-size 1GiB \
  --filter-mtime-after 2025-01-01T00:00:00Z \
  s3://my-bucket/data/
```

### Sorting

```bash
# Sort by size (largest first)
s3ls --recursive --sort size --reverse s3://my-bucket/

# Sort by date, then by key
s3ls --recursive --sort date,key s3://my-bucket/

# Stream results without sorting (lower memory usage for huge buckets)
s3ls --recursive --no-sort s3://my-bucket/
```

### Display Options

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

### JSON Output

```bash
# NDJSON output (one JSON object per line) — includes all available fields
s3ls --recursive --json s3://my-bucket/

# Pipe to jq for further processing
s3ls --recursive --json s3://my-bucket/ | jq 'select(.Size > 1000000)'

# JSON output with summary
s3ls --recursive --json --summarize s3://my-bucket/
```

JSON output is **S3 API-compliant** — field names, types, and structure match the actual S3 `ListObjectsV2` and `ListObjectVersions` API responses:

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

This means you can process s3ls JSON output with the same tools and scripts that parse `aws s3api` responses — no field name translation required. All available fields are always included regardless of `--show-*` flags.

### Version Listing

```bash
# List all object versions including delete markers
s3ls --recursive --all-versions s3://my-bucket/

# Show which version is latest
s3ls --recursive --all-versions --show-is-latest s3://my-bucket/

# Hide delete markers
s3ls --recursive --all-versions --hide-delete-marker s3://my-bucket/
```

### Depth-Limited Recursive Listing

```bash
# Recursive but only 2 levels deep — shows PRE for deeper prefixes
s3ls --recursive --max-depth 2 s3://my-bucket/

# Useful for exploring bucket structure without listing everything
s3ls --recursive --max-depth 1 s3://my-bucket/data/
```

### Bucket Listing

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

### S3-Compatible Services

```bash
# MinIO
s3ls --target-endpoint-url http://localhost:9000 \
     --target-force-path-style \
     --target-access-key minioadmin \
     --target-secret-access-key minioadmin \
     s3://my-bucket/

# Use a named AWS profile
s3ls --target-profile production s3://my-bucket/
```

## Performance Tuning

s3ls defaults are tuned for most workloads, but you can adjust for specific scenarios:

| Option | Default | Description |
|--------|---------|-------------|
| `--max-parallel-listings` | 32 | Number of concurrent S3 API listing calls |
| `--max-parallel-listing-max-depth` | 2 | How deep to discover prefixes before switching to sequential |
| `--no-sort` | off | Stream results directly without buffering in memory |

For very large buckets (millions of objects), consider:

```bash
# Stream results without sorting to avoid memory buffering
s3ls --recursive --no-sort s3://huge-bucket/

# Increase parallelism for deep hierarchies
s3ls --recursive --max-parallel-listings 64 --max-parallel-listing-max-depth 3 s3://deep-bucket/
```

## Comparison with Other Tools

There are several tools that can list S3 objects, but none of them expose the full depth of information that the S3 API actually returns. s3ls was built to fill that gap.

### Feature Comparison

| Feature | s3ls | aws s3 ls | aws s3api | s5cmd ls | rclone lsl | s3cmd ls |
|---------|:----:|:---------:|:---------:|:--------:|:----------:|:--------:|
| **Speed** | | | | | | |
| Parallel listing | 32 concurrent | Sequential | Sequential | Parallel | Parallel | Sequential |
| Throughput (200K objects) | ~1.4s | ~30s | ~25s | ~3-5s | ~10s | ~40s+ |
| Streaming mode (no buffering) | `--no-sort` | - | - | - | - | - |
| **Object Metadata** | | | | | | |
| Key, Size, LastModified | Yes | Yes | Yes | Yes | Yes | Yes |
| ETag | `--show-etag` | - | Yes | - | - | Yes |
| StorageClass | `--show-storage-class` | - | Yes | Yes | - | Yes |
| ChecksumAlgorithm | `--show-checksum-algorithm` | - | Yes | - | - | - |
| ChecksumType | `--show-checksum-type` | - | Yes | - | - | - |
| Owner (DisplayName + ID) | `--show-owner` | - | Yes | - | - | - |
| RestoreStatus | `--show-restore-status` | - | - | - | - | - |
| **Versioning** | | | | | | |
| List all versions | `--all-versions` | - | Yes | - | - | - |
| Show IsLatest | `--show-is-latest` | - | Yes | - | - | - |
| Hide delete markers | `--hide-delete-marker` | - | - | - | - | - |
| **Filtering** | | | | | | |
| Regex include/exclude | Yes | - | - | Glob only | Filter | Glob only |
| Modified time range | Yes | - | - | Before/After | Time range | After only |
| Size range | Yes | - | - | Size filter | Size filter | - |
| Storage class filter | Yes | - | - | - | - | - |
| **Output** | | | | | | |
| Tab-delimited text | Yes | Space-padded | - | Space-padded | Space-padded | Space-padded |
| NDJSON (S3 API-aligned) | `--json` | - | JSON | `--json` | `--json` | - |
| Column headers | `--header` | - | - | - | - | - |
| Human-readable sizes | `--human-readable` | Yes | - | Yes | - | Yes |
| Summary statistics | `--summarize` | Yes | - | - | - | - |
| Full path / relative path | Default full | Relative | Full | Full | Relative | Relative |
| **Listing Control** | | | | | | |
| Depth-limited recursion | `--max-depth` | - | - | - | `--max-depth` | - |
| PRE at depth boundary | Yes | - | - | - | - | - |
| Non-recursive (PRE display) | Yes | Yes | Delimiter | Yes | - | Yes |
| **Bucket Listing** | | | | | | |
| List buckets | Yes | Yes | Yes | Yes | Yes | Yes |
| Filter by name prefix | `--bucket-name-prefix` | - | `--prefix` | - | - | - |
| Show bucket ARN | `--show-bucket-arn` | - | Yes | - | - | - |
| Express One Zone buckets | Yes | - | Yes | - | - | - |
| **Sorting** | | | | | | |
| Multi-column sort | Up to 2 fields | - | - | - | - | - |
| Sort by key/size/date | Yes | Key only | - | - | - | - |
| Reverse sort | Yes | - | - | - | - | - |
| **Infrastructure** | | | | | | |
| Custom endpoint (MinIO, etc.) | Yes | Yes | Yes | Yes | Yes | Yes |
| AWS profile support | Yes | Yes | Yes | Yes | Yes | Yes |
| S3 Transfer Acceleration | Yes | Yes | Yes | - | - | - |
| Requester-pays | Yes | Yes | Yes | - | - | - |
| Shell completions | bash/zsh/fish/pwsh | - | bash/zsh | - | bash | - |
| Single static binary | Yes | No (Python) | No (Python) | Yes | Yes | No (Python) |

### Key Differentiators

**No other tool exposes all of this from a single command:**

- **ChecksumAlgorithm and ChecksumType** — S3 added these fields for data integrity verification (CRC32, CRC32C, SHA1, SHA256, CRC64NVME). `aws s3api` returns them in JSON, but no CLI listing tool surfaces them in a usable way. s3ls does.

- **RestoreStatus** — When you restore objects from Glacier/Deep Archive, S3 tracks whether the restore is in progress and when it expires. This requires the `OptionalObjectAttributes=RestoreStatus` request parameter. s3ls is the only listing tool that supports this.

- **Owner information** — S3 can return the object owner's DisplayName and ID, but it requires `fetch-owner=true` on the API call (which costs slightly more). Most tools skip this entirely. s3ls exposes it with `--show-owner` and renders it as a nested `Owner` object in JSON, matching the S3 API structure.

- **Hide delete markers** — When listing versions, delete markers clutter the output. `aws s3api` returns them but offers no way to filter them out. s3ls provides `--hide-delete-marker`.

- **Depth-limited recursion with PRE** — `--max-depth` with `--recursive` lets you explore a bucket hierarchy level by level, showing "PRE" entries at the boundary just like non-recursive mode. This is invaluable for understanding the structure of unfamiliar buckets without listing millions of objects.

- **S3 API-aligned JSON** — The `--json` output uses PascalCase keys (`Key`, `Size`, `LastModified`, `ETag`, `StorageClass`) matching the S3 API exactly. Other tools use their own key naming conventions, making it harder to integrate with tools that already parse S3 API responses.

- **Streaming mode** — `--no-sort` writes results directly as they arrive from S3, with zero memory buffering. For a bucket with 10 million objects, this is the difference between needing gigabytes of RAM and needing almost none.

## Shell Completions

```bash
# Generate completions for your shell
s3ls --auto-complete-shell bash > /etc/bash_completion.d/s3ls
s3ls --auto-complete-shell zsh > ~/.zfunc/_s3ls
s3ls --auto-complete-shell fish > ~/.config/fish/completions/s3ls.fish
```

## License

Apache-2.0
