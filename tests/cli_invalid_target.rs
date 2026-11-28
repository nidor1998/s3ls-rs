//! Process-level tests for target validation. The target is rejected while
//! the arguments are parsed, so no S3 access is needed.
//!
//! The prefix of a target is used exactly as given, so `?` and `#` are
//! ordinary prefix characters after the `/` that ends the bucket name.
//! Directly after the bucket name they are neither part of a bucket name
//! nor a prefix, and the target is rejected with an argument error.

use std::process::Command;

fn run_s3ls(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_s3ls"))
        .args(args)
        .output()
        .expect("failed to run s3ls")
}

#[test]
fn question_mark_or_hash_after_bucket_is_an_argument_error() {
    for target in [
        "s3://my-bucket?old",
        "s3://my-bucket#old",
        "s3://my-bucket?",
        "s3://my-bucket#",
        "s3://my-bucket?old/",
        "s3://my-bucket#a/b?c",
    ] {
        let output = run_s3ls(&[target]);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(
            output.status.code(),
            Some(2),
            "target {target}: expected exit 2\nstderr: {stderr}"
        );
        assert!(
            stderr.contains("'?' or '#' directly after the bucket name is not allowed"),
            "target {target}: unexpected stderr: {stderr}"
        );
        assert!(
            !stderr.contains("panicked"),
            "target {target}: panicked\nstderr: {stderr}"
        );
        assert!(
            output.stdout.is_empty(),
            "target {target}: nothing must be listed"
        );
    }
}

#[test]
fn target_without_s3_scheme_is_an_argument_error() {
    for target in ["my-bucket?old", "my-bucket/prefix", "s3://"] {
        let output = run_s3ls(&[target]);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(
            output.status.code(),
            Some(2),
            "target {target}: expected exit 2\nstderr: {stderr}"
        );
        assert!(
            stderr.contains("target must be an S3 path"),
            "target {target}: unexpected stderr: {stderr}"
        );
    }
}
