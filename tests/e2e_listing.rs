#![cfg(e2e_test)]

mod common;

use common::*;
use s3ls_rs::create_pipeline_cancellation_token;

/// Binary-path smoke test.
///
/// Uploads three objects in reverse alphabetical order and runs
/// `s3ls --recursive`, asserting via `assert_key_order` that s3ls's default
/// key-sort produces alphabetical output. This double-purposes as framework
/// plumbing verification (TestHelper, bucket lifecycle, run_s3ls, S3lsOutput,
/// assert_key_order, e2e_timeout!, BucketGuard) and as a regression check
/// against s3ls's sort stability.
#[tokio::test]
async fn e2e_binary_smoke() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Upload in REVERSE alphabetical order — default key sort must still
        // produce a, b, c.
        helper.put_object(&bucket, "c.txt", b"ccc".to_vec()).await;
        helper.put_object(&bucket, "b.txt", b"bb".to_vec()).await;
        helper.put_object(&bucket, "a.txt", b"a".to_vec()).await;

        let target = format!("s3://{bucket}/");
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive"]);

        assert!(
            output.status.success(),
            "s3ls exited non-zero: {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            output.stdout,
            output.stderr
        );

        assert_key_order(&output.stdout, &["a.txt", "b.txt", "c.txt"]);
    });

    _guard.cleanup().await;
}

/// Programmatic-path smoke test.
///
/// Builds a `Config` via `TestHelper::build_config`, constructs a
/// `ListingPipeline`, and runs it. Asserts only that the pipeline returned
/// `Ok(())` — rendered output is the binary path's concern. This catches
/// API-drift bugs at the `s3ls_rs` public-API surface (`Config`,
/// `ListingPipeline::new`, `ListingPipeline::run`, cancellation token).
#[tokio::test]
async fn e2e_programmatic_smoke() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper
            .put_object(&bucket, "file.txt", b"hello".to_vec())
            .await;

        let target = format!("s3://{bucket}/");
        let config = TestHelper::build_config(vec![target.as_str(), "--recursive"]);
        let token = create_pipeline_cancellation_token();
        let pipeline = s3ls_rs::ListingPipeline::new(config, token);

        pipeline.run().await.expect("pipeline run failed");
    });

    _guard.cleanup().await;
}
