#![cfg(e2e_test)]

mod common;

use common::TestHelper;

#[tokio::test]
async fn e2e_sanity() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    assert!(bucket.starts_with("s3ls-e2e-"));
    assert_eq!(bucket.len(), 9 + 36); // "s3ls-e2e-" + UUID v4
}
