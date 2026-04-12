# E2E Tests

End-to-end tests for s3ls-rs. Gated behind `--cfg e2e_test` so they only run
when explicitly requested and never interfere with `cargo test`.

## Test suites

| File | Tests | Description |
|------|------:|-------------|
| `e2e_listing.rs` | 13 | Listing smoke tests, exit codes (0/1/2), timeouts/retries, Express One Zone with prefix |
| `e2e_filters.rs` | 14 | Per-filter tests (regex, size, mtime, storage-class), combinations, max-depth/no-sort smokes |
| `e2e_filters_versioned.rs` | 9 | Versioning-specific filter tests, delete-marker handling, pagination (parallel + sequential) |
| `e2e_display.rs` | 17 | Display flags (--show-*, --header, --summarize, --human-readable), PRE/DELETE row padding, bucket listing display |
| `e2e_sort.rs` | 11 | Sort by key/size/date (asc/desc), multi-column tiebreak, --no-sort, versioning secondary sort, bucket listing sort |
| `e2e_bucket_listing.rs` | 8 | Bucket listing: JSON shape, --bucket-name-prefix, combined flags, --no-sort, Express One Zone (listing + prefix filter) |
| `e2e_large_listing.rs` | 1 | 16K-object listing completeness (full recursive, prefix, max-depth, max-parallel-listing-max-depth) |
| `e2e_edge_cases.rs` | 12 | UTF-8 keys (2-4 byte), control chars, --raw-output, slash-suffix keys, request-payer, versioned summary with DMs |

## Prerequisites

### 1. AWS profile

```bash
aws configure --profile s3ls-e2e-test
```

The framework loads credentials from the `s3ls-e2e-test` profile and applies
`--target-profile s3ls-e2e-test` to every s3ls invocation (both binary and
programmatic paths). The region from the profile is used to create test
buckets.

### 2. IAM permissions

The profile's principal needs the following S3 permissions:

- `s3:CreateBucket`
- `s3:DeleteBucket`
- `s3:PutObject`
- `s3:GetObject`
- `s3:ListBucket`
- `s3:ListAllMyBuckets`
- `s3:DeleteObject`
- `s3:ListBucketVersions`
- `s3:PutBucketVersioning`
- `s3:ListDirectoryBuckets` *(for Express One Zone tests)*
- `s3express:CreateSession` *(for Express One Zone object listing)*

No bucket pre-creation is required — the framework creates a fresh bucket of
the form `s3ls-e2e-{uuid}` per test and cleans it up at the end.

### 3. Express One Zone (optional)

Some tests exercise Express One Zone (directory) buckets. These tests skip
gracefully if the region doesn't support Express One Zone or bucket creation
fails. No manual setup is needed — the tests handle AZ discovery and
fallback automatically.

## Running

Run all e2e tests:

```bash
RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture
```

Run one file:

```bash
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_listing -- --nocapture
```

Run one test:

```bash
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_listing e2e_binary_smoke -- --nocapture
```

`--nocapture` is recommended so pipeline output and debug prints surface
immediately on failure.

### Trace logging

Set the `E2E_TEST_LOG_LEVEL` environment variable to inject a verbosity flag
into every s3ls invocation:

```bash
# Verbose tracing (shows S3 API calls)
E2E_TEST_LOG_LEVEL='-vvv' RUSTFLAGS='--cfg e2e_test' \
  cargo test --test e2e_listing -- --nocapture

# Quiet (suppress warnings)
E2E_TEST_LOG_LEVEL='-qq' RUSTFLAGS='--cfg e2e_test' \
  cargo test --test e2e_listing -- --nocapture
```

Valid values: `-v`, `-vv`, `-vvv`, `-q`, `-qq`. Invalid values are silently
ignored.

## Costs and caveats

- Tests hit real AWS S3 and create real buckets. Most tests cost well under
  $0.01. The large-listing test (`e2e_large_listing.rs`) uploads ~16K objects
  and costs ~$0.16 per run.
- AWS eventual consistency can cause occasional flakes. Retry once; if it
  fails again, investigate.
- Tests run against whatever region is configured in the `s3ls-e2e-test`
  profile — pick a region you control and can be billed from.
- The large-listing test uses a 300-second timeout (not the standard 60s)
  and takes ~40 seconds to complete.

## Cleaning leaked buckets

Each test uses an explicit `BucketGuard::cleanup().await` instead of a `Drop`
impl. This is intentional: a `Drop` impl that calls `block_on` during test
panic unwinding can deadlock or double-panic, losing the original failure
message. The trade is that if a test panics before reaching `cleanup()`, its
bucket is leaked.

To clean leaked buckets:

```bash
# List any leaked e2e buckets
aws s3api list-buckets --profile s3ls-e2e-test \
  --query 'Buckets[?starts_with(Name, `s3ls-e2e-`)].Name' \
  --output text

# For each leaked *unversioned* bucket:
aws s3 rb s3://s3ls-e2e-<uuid> --force --profile s3ls-e2e-test
```

### Versioned buckets

`aws s3 rb --force` only deletes current object versions. On a versioned
bucket it leaves behind non-current versions and delete markers, so the
final `DeleteBucket` call fails with `BucketNotEmpty`. Some e2e tests
create versioned buckets, so a leaked bucket may need this path instead:

```bash
BUCKET=s3ls-e2e-<uuid>
PROFILE=s3ls-e2e-test

# Delete all object versions
aws s3api delete-objects --bucket "$BUCKET" --profile "$PROFILE" \
  --delete "$(aws s3api list-object-versions \
    --bucket "$BUCKET" --profile "$PROFILE" \
    --output json \
    --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}')"

# Delete all delete markers
aws s3api delete-objects --bucket "$BUCKET" --profile "$PROFILE" \
  --delete "$(aws s3api list-object-versions \
    --bucket "$BUCKET" --profile "$PROFILE" \
    --output json \
    --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}')"

# Finally, delete the empty bucket
aws s3api delete-bucket --bucket "$BUCKET" --profile "$PROFILE"
```

If the bucket holds more than 1000 versions + delete markers combined,
repeat the two `delete-objects` calls until `list-object-versions` returns
an empty result (each call deletes up to 1000 keys per request).

### Directory buckets

Express One Zone directory buckets (names ending with `--x-s3`) can be
deleted directly if empty:

```bash
aws s3api delete-bucket --bucket s3ls-e2e-<uuid>--<az>--x-s3 --profile s3ls-e2e-test
```

If the bucket contains objects, delete them first:

```bash
aws s3 rm s3://s3ls-e2e-<uuid>--<az>--x-s3 --recursive --profile s3ls-e2e-test
aws s3api delete-bucket --bucket s3ls-e2e-<uuid>--<az>--x-s3 --profile s3ls-e2e-test
```

## CI

E2E tests are **not** run in CI. The existing GitHub Actions workflows run
`cargo test` without the `--cfg e2e_test` flag, so these tests stay invisible
to CI. Wiring them in is tracked separately and requires decisions about
secrets, cost budget, flake retries, and which events trigger the suite.
