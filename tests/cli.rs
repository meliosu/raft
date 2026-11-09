//! Exercise the actual binary: configuration stays on disk, Raft state does not.
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use tempfile::tempdir;

struct NodeProcess(Child);

impl Drop for NodeProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &str,
) -> std::io::Result<(u16, Value)> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(100))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("invalid HTTP response");
    let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
    Ok((
        status,
        serde_json::from_str(body).expect("invalid JSON response"),
    ))
}

#[test]
fn in_memory_binary_reads_config_and_commits_without_a_state_directory() {
    let dir = tempdir().unwrap();
    let config = dir.path().join("config.yaml");
    let state = dir.path().join("unused-state");
    let grpc = TcpListener::bind("127.0.0.1:0").unwrap();
    let http = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = http.local_addr().unwrap();
    // JSON is also valid YAML, avoiding path quoting assumptions in the fixture.
    std::fs::write(
        &config,
        json!({
            "node_id": 1, "grpc_bind": grpc.local_addr().unwrap(), "http_bind": address,
            "data_dir": state, "heartbeat_interval_ms": 20, "election_timeout_min_ms": 100,
            "election_timeout_max_ms": 200, "rpc_timeout_ms": 50,
            "nodes": [{"id": 1, "address": format!("http://{}", grpc.local_addr().unwrap())}]
        })
        .to_string(),
    )
    .unwrap();
    drop(grpc);
    drop(http);
    let mut child = NodeProcess(
        Command::new(env!("CARGO_BIN_EXE_raft"))
            .arg("--in-memory")
            .arg("--config")
            .arg(&config)
            .env("RUST_LOG", "error")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "node exited before becoming leader"
        );
        if let Ok((200, status)) = request(address, "GET", "/api/status", "")
            && status["role"] == "leader"
            && status["commit_index"] == 1
        {
            break;
        }
        assert!(Instant::now() < deadline, "node did not become leader");
        sleep(Duration::from_millis(10));
    }
    let (status, proposal) =
        request(address, "POST", "/api/logs", r#"{"command":"volatile"}"#).unwrap();
    assert_eq!(status, 201);
    assert_eq!(proposal["index"], 2);
    let (status, logs) = request(address, "GET", "/api/logs", "").unwrap();
    assert_eq!(status, 200);
    assert_eq!(logs["entries"][1]["command"], "volatile");
    assert_eq!(logs["entries"][1]["committed"], true);
    drop(child);
    assert!(!state.exists());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1); // config only
}

#[test]
fn in_memory_does_not_bypass_missing_or_invalid_configuration() {
    let dir = tempdir().unwrap();
    let config = dir.path().join("config.yaml");
    for (contents, error) in [
        (None, "reading"),
        (Some("unsupported_option: true"), "unknown field"),
    ] {
        if let Some(contents) = contents {
            std::fs::write(&config, contents).unwrap();
        }
        let output = Command::new(env!("CARGO_BIN_EXE_raft"))
            .arg("--in-memory")
            .arg("--config")
            .arg(&config)
            .env("RUST_LOG", "error")
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(error), "expected {error:?}, got {stderr}");
    }
}
