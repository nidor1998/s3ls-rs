//! Process-level regression tests for broken-pipe handling on paths that
//! need no S3 access. The object/bucket listing counterparts live in
//! `tests/e2e_edge_cases.rs` (`e2e_edge_case_*_broken_pipe`).

use std::io::Read;
use std::process::{Command, Stdio};

/// Shell-completion generation piped to a reader that exits without
/// consuming the script must exit 0 instead of panicking inside
/// clap_complete's stdout writer (`failed to write completion file:
/// Broken pipe`).
#[test]
fn completion_script_to_closed_pipe_exits_zero() {
    for shell in ["bash", "zsh", "fish", "powershell", "elvish"] {
        // Close the read end before the child is spawned so every stdout
        // write is guaranteed to hit a pipe with no reader.
        let (reader, writer) = std::io::pipe().expect("failed to create pipe");
        drop(reader);

        let mut child = Command::new(env!("CARGO_BIN_EXE_s3ls"))
            .args(["--auto-complete-shell", shell])
            .stdout(writer)
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn s3ls");

        let status = child.wait().expect("failed to wait for s3ls");
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .expect("stderr was piped")
            .read_to_string(&mut stderr)
            .expect("failed to read stderr");

        assert!(
            status.success(),
            "shell {shell}: expected exit 0 on closed pipe, got {:?}\nstderr: {stderr}",
            status.code()
        );
        assert!(
            !stderr.contains("panicked"),
            "shell {shell}: panicked on closed pipe\nstderr: {stderr}"
        );
    }
}

/// The completion script must still be emitted in full when stdout is
/// actually read.
#[test]
fn completion_script_still_prints_when_read() {
    let output = Command::new(env!("CARGO_BIN_EXE_s3ls"))
        .args(["--auto-complete-shell", "bash"])
        .output()
        .expect("failed to run s3ls");

    assert!(output.status.success(), "expected exit 0");
    let script = String::from_utf8_lossy(&output.stdout);
    assert!(
        script.contains("_s3ls"),
        "expected bash completion function in output, got: {script:?}"
    );
}
