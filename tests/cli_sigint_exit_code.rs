//! Process-level regression tests for SIGINT (Ctrl+C) exit-code handling.
//!
//! `s3ls` catches Ctrl+C, cancels the listing pipeline, and must then exit
//! gracefully with code 130 (128 + SIGINT), the conventional shell encoding
//! for a run interrupted by the user. These tests run the real binary
//! against a minimal in-process S3 endpoint (no AWS access needed): the
//! endpoint keeps returning truncated pages so the listing runs until the
//! test sends SIGINT.
//!
//! Covers `src/bin/s3ls/main.rs` (`is_ctrl_c_received` → exit
//! `SIGINT_EXIT_CODE`) and `src/bin/s3ls/ctrl_c_handler/mod.rs`.

use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Which listing API the fake endpoint emulates.
#[derive(Clone, Copy)]
enum ListingKind {
    Objects,
    #[cfg(target_family = "unix")]
    Versions,
}

/// Handle to a fake S3 endpoint running on a background thread.
struct FakeS3 {
    endpoint: String,
    pages_served: Arc<AtomicUsize>,
}

/// Serve canned S3 list responses over plain HTTP/1.1, one request per
/// connection. Every page contains one object and advances its
/// continuation token / key marker (the pipeline refuses repeated
/// tokens). With `total_pages = Some(n)` the n-th page is final
/// (`IsTruncated` false); with `None` the listing is endless, so the
/// child only stops when it is signalled.
fn spawn_fake_s3(kind: ListingKind, total_pages: Option<usize>) -> FakeS3 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind fake S3 listener");
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let pages_served = Arc::new(AtomicUsize::new(0));

    let served = Arc::clone(&pages_served);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
            read_request_head(&mut stream);

            let page = served.load(Ordering::SeqCst);
            let truncated = total_pages.is_none_or(|total| page + 1 < total);
            let body = match kind {
                ListingKind::Objects => objects_page(page, truncated),
                #[cfg(target_family = "unix")]
                ListingKind::Versions => versions_page(page, truncated),
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len(),
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
            served.fetch_add(1, Ordering::SeqCst);
        }
    });

    FakeS3 {
        endpoint,
        pages_served,
    }
}

/// Read the request line and headers; list requests carry no body.
fn read_request_head(stream: &mut TcpStream) {
    let mut head = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                head.extend_from_slice(&buf[..n]);
                if head.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

fn objects_page(page: usize, truncated: bool) -> String {
    let next_token = if truncated {
        format!("<NextContinuationToken>token-{page}</NextContinuationToken>")
    } else {
        String::new()
    };
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>fake-sigint-bucket</Name><Prefix></Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>{truncated}</IsTruncated>{next_token}<Contents><Key>page-{page}.txt</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>&quot;0123456789abcdef0123456789abcdef&quot;</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"#
    )
}

#[cfg(target_family = "unix")]
fn versions_page(page: usize, truncated: bool) -> String {
    let next_markers = if truncated {
        format!(
            "<NextKeyMarker>page-{page}.txt</NextKeyMarker><NextVersionIdMarker>version-{page}</NextVersionIdMarker>"
        )
    } else {
        String::new()
    };
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>fake-sigint-bucket</Name><Prefix></Prefix><KeyMarker></KeyMarker><VersionIdMarker></VersionIdMarker><MaxKeys>1000</MaxKeys><IsTruncated>{truncated}</IsTruncated>{next_markers}<Version><Key>page-{page}.txt</Key><VersionId>version-{page}</VersionId><IsLatest>true</IsLatest><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>&quot;0123456789abcdef0123456789abcdef&quot;</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Version></ListVersionsResult>"#
    )
}

/// Spawn the s3ls binary pointed at the fake endpoint with static
/// credentials, so no AWS configuration on the host is consulted.
///
/// `stdout` controls capture: SIGINT tests discard stdout (the listing
/// never completes), the control test captures it.
fn spawn_s3ls(endpoint: &str, extra_args: &[&str], stdout: Stdio) -> Child {
    let mut args = vec![
        "s3://fake-sigint-bucket/",
        "--target-endpoint-url",
        endpoint,
        "--target-force-path-style",
        "--target-access-key",
        "fake-access-key",
        "--target-secret-access-key",
        "fake-secret-key",
        "--target-region",
        "us-east-1",
    ];
    args.extend_from_slice(extra_args);

    Command::new(env!("CARGO_BIN_EXE_s3ls"))
        .args(&args)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn s3ls")
}

/// Wait until the fake endpoint has served at least `pages` pages, so the
/// child is provably inside the listing loop (its Ctrl+C handler is
/// installed before the pipeline starts, thus long since registered).
fn wait_for_pages_served(fake: &FakeS3, pages: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while fake.pages_served.load(Ordering::SeqCst) < pages {
        assert!(
            Instant::now() < deadline,
            "fake S3 served only {} page(s) before timeout",
            fake.pages_served.load(Ordering::SeqCst)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Wait for the child to exit within `deadline` — SIGINT must terminate
/// the process promptly, not hang it. Kills the child on timeout.
fn wait_with_deadline(child: &mut Child, deadline: Duration) -> std::process::ExitStatus {
    let end = Instant::now() + deadline;
    loop {
        if let Some(status) = child.try_wait().expect("failed to poll s3ls") {
            return status;
        }
        if Instant::now() >= end {
            let _ = child.kill();
            let _ = child.wait();
            panic!("s3ls did not exit within {deadline:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Read the child's stderr after it has exited.
fn read_stderr(child: &mut Child) -> String {
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    stderr
}

#[cfg(target_family = "unix")]
fn send_sigint(child: &Child) {
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(child.id() as i32),
        nix::sys::signal::Signal::SIGINT,
    )
    .expect("failed to send SIGINT to s3ls");
}

/// Shared body of the SIGINT tests: start an endless listing, interrupt it
/// mid-flight, and require a graceful exit with code 130 — `code()` is
/// `None` for a raw signal kill, so this also proves the signal was caught
/// rather than terminating the process directly — and a quiet stderr.
#[cfg(target_family = "unix")]
fn assert_sigint_exits_130(kind: ListingKind, extra_args: &[&str], label: &str) {
    let fake = spawn_fake_s3(kind, None);
    let mut child = spawn_s3ls(&fake.endpoint, extra_args, Stdio::null());

    wait_for_pages_served(&fake, 3);
    send_sigint(&child);

    let status = wait_with_deadline(&mut child, Duration::from_secs(15));
    let stderr = read_stderr(&mut child);

    assert_eq!(
        status.code(),
        Some(130),
        "[{label}] expected graceful exit 130 after SIGINT, got {status:?}\nstderr: {stderr}"
    );
    assert!(
        !stderr.contains("panicked") && !stderr.contains("s3ls failed"),
        "[{label}] SIGINT should terminate quietly\nstderr: {stderr}"
    );
}

/// Ctrl+C during a plain (non-recursive, sequential) object listing.
#[test]
#[cfg(target_family = "unix")]
fn sigint_during_object_listing_exits_130() {
    assert_sigint_exits_130(ListingKind::Objects, &[], "objects");
}

/// Ctrl+C during `--all-versions` listing (ListObjectVersions paging loop).
#[test]
#[cfg(target_family = "unix")]
fn sigint_during_versions_listing_exits_130() {
    assert_sigint_exits_130(ListingKind::Versions, &["--all-versions"], "versions");
}

/// Ctrl+C during `--recursive` listing (parallel listing dispatch path).
#[test]
#[cfg(target_family = "unix")]
fn sigint_during_recursive_listing_exits_130() {
    assert_sigint_exits_130(ListingKind::Objects, &["--recursive"], "recursive");
}

/// Control: the same harness without SIGINT completes and exits 0 — the
/// SIGINT handling must not affect uninterrupted runs.
#[test]
fn listing_without_sigint_exits_zero() {
    let fake = spawn_fake_s3(ListingKind::Objects, Some(3));
    let mut child = spawn_s3ls(&fake.endpoint, &[], Stdio::piped());

    let status = wait_with_deadline(&mut child, Duration::from_secs(30));
    let stderr = read_stderr(&mut child);
    let mut stdout = String::new();
    if let Some(mut pipe) = child.stdout.take() {
        let _ = pipe.read_to_string(&mut stdout);
    }

    assert_eq!(
        status.code(),
        Some(0),
        "expected exit 0, got {status:?}\nstderr: {stderr}"
    );
    assert_eq!(fake.pages_served.load(Ordering::SeqCst), 3);
    for key in ["page-0.txt", "page-1.txt", "page-2.txt"] {
        assert!(stdout.contains(key), "stdout missing {key}:\n{stdout}");
    }
}
