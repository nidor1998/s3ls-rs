# End-to-End Tests

## Warning

These tests will create and delete AWS resources (S3 buckets and objects), which will result in costs on your AWS account.

These tests are designed to be run against a real AWS account. If any of the tests fail, they may leave resources in your AWS account, such as S3 buckets and their contents.

## Running the tests against AWS

Before running the tests, you need to set up your AWS credentials. Create a profile named `s3ls-e2e-test` with the AWS CLI:

```bash
aws configure --profile s3ls-e2e-test
```

Then run the tests with the `e2e_test` cfg flag:

```bash
# Run all E2E tests
RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*'

# Run a specific test suite
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_listing
```

### Region

The tests run against the region configured in the `s3ls-e2e-test` profile. Some tests exercise Express One Zone (directory) buckets and skip gracefully if the region doesn't support them.

### Environment variables

| Variable | Behavior |
|---|---|
| `E2E_TEST_LOG_LEVEL` | Injects a verbosity flag (`-v`, `-vv`, `-vvv`, `-q`, `-qq`) into every s3ls invocation. Invalid values are silently ignored. |

## Notes

These tests create and delete S3 buckets. Occasionally tests may fail due to eventual consistency in AWS (for example, a newly created bucket may not be immediately visible). In such cases, the tests will typically pass on a subsequent run.

Each test creates a fresh bucket named `s3ls-e2e-{uuid}` and cleans it up at the end. If a test panics before cleanup, the bucket is leaked — delete leaked `s3ls-e2e-*` buckets manually (versioned buckets also require deleting all object versions and delete markers before the bucket can be removed).
