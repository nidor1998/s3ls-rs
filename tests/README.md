# E2E Tests

End-to-end tests for s3ls-rs. Gated behind `--cfg e2e_test` so they only run
when explicitly requested and never interfere with `cargo test`.

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
- `s3:DeleteObject`
- `s3:ListBucketVersions`
- `s3:PutBucketVersioning`
- `s3:PutBucketPolicy` *(forward-compatibility for future error-path tests)*
- `s3:DeleteBucketPolicy` *(forward-compatibility)*

No bucket pre-creation is required — the framework creates a fresh bucket of
the form `s3ls-e2e-{uuid}` per test and cleans it up at the end.

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

## Costs and caveats

- Tests hit real AWS S3 and create real buckets. Expect small charges
  (bucket ops, short-lived objects).
- AWS eventual consistency can cause occasional flakes. Retry once; if it
  fails again, investigate.
- Tests run against whatever region is configured in the `s3ls-e2e-test`
  profile — pick a region you control and can be billed from.

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

# For each leaked bucket:
aws s3 rb s3://s3ls-e2e-<uuid> --force --profile s3ls-e2e-test
```

## CI

E2E tests are **not** run in CI. The existing GitHub Actions workflows run
`cargo test` without the `--cfg e2e_test` flag, so these tests stay invisible
to CI. Wiring them in is tracked separately and requires decisions about
secrets, cost budget, flake retries, and which events trigger the suite.
