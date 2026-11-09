# Raft replicated log in Rust

A small Raft node using Tokio, tonic/prost gRPC, Axum, and
`log`/`env_logger`. The cluster has fixed membership. Each node starts as a
follower, uses a randomized election timeout, votes once per term, and becomes
leader after receiving a strict majority of the configured members' votes.

The gRPC service has `RequestVote` and `AppendEntries`. Empty AppendEntries
requests serve as heartbeats; nonempty requests replicate JSON commands. Logs,
terms, and votes survive restarts. This is a replicated log, not an application
state machine: commands are committed but are not executed. Snapshots, log
compaction, membership changes, client deduplication, and linearizable reads are
not implemented. The log is retained in memory and the journal grows on disk.

Run locally with Rust 1.88 or newer (a vendored `protoc` is used automatically):

```bash
cargo build --locked
# Run each command in a separate terminal.
RUST_LOG=info ./target/debug/raft --config config/node1.yaml
RUST_LOG=info ./target/debug/raft --config config/node2.yaml
RUST_LOG=info ./target/debug/raft --config config/node3.yaml
curl http://127.0.0.1:8081/api/status
```

`GET /api/status` returns a local snapshot:

```json
{
    "node_id": 1,
    "role": "follower",
    "current_term": 4,
    "leader_id": 2,
    "last_log_index": 12,
    "commit_index": 10
}
```

Roles are `follower`, `candidate`, or `leader`. `leader_id` is `null` until the
node wins an election or hears a current leader's heartbeat. This status is
local knowledge: during a partition an isolated old leader can still report
`leader` while the majority elects a leader in a higher term.

## Submitting and inspecting logs

Submit a JSON command to the leader's HTTP port:

```bash
curl -i http://127.0.0.1:8082/api/logs \
  -H 'Content-Type: application/json' \
  -d '{"command":{"operation":"set","key":"hello","value":"world"}}'
# HTTP 201: {"index":13,"term":4}
```

`POST /api/logs` returns **201 only after the entry is durably replicated to a
strict majority and committed**. A follower returns **409** with
`{"error":"not_leader","leader_id":2}` (or a null leader ID). There is no
automatic forwarding. If leadership is lost or the five-second wait expires,
**504** with `error: "outcome_unknown"` means the entry **may still commit**.
Retrying can produce duplicate commands. **503** indicates a busy or unavailable
node; **400** rejects invalid command size/depth or pagination. Malformed JSON
uses Axum's validation errors; oversized HTTP bodies return **413**.

Commands may be any JSON value, including `null`, and are limited to 16 KiB of
compact JSON and 64 nesting levels. HTTP request bodies are limited to 17 KiB,
including the command wrapper and whitespace. At most 128 client proposals can
wait for commitment on a node.

Inspect each node independently, including followers:

```bash
curl 'http://127.0.0.1:8081/api/logs?from=1&limit=100'
curl 'http://127.0.0.1:8082/api/logs?from=1&limit=100'
curl 'http://127.0.0.1:8083/api/logs?from=1&limit=100'
```

Example response:

```json
{
    "node_id": 1,
    "current_term": 4,
    "commit_index": 2,
    "last_log_index": 3,
    "entries": [
        {"index":1,"term":4,"kind":"noop","command":null,"committed":true},
        {"index":2,"term":4,"kind":"command","command":{"key":"hello"},"committed":true},
        {"index":3,"term":4,"kind":"command","command":null,"committed":false}
    ],
    "next_index": null
}
```

Indexes start at 1. `from` defaults to 1; `limit` defaults to 100 and must be
between 1 and 100. Follow `next_index` for another page; it is null at the local
tail. Each page is a consistent local snapshot, but pages are not a stable
cross-request snapshot: uncommitted suffixes can be replaced. A no-op is appended
on every election to commit entries inherited from prior leaders. `kind`
distinguishes no-ops from client `null` commands. These inspection endpoints
are **not linearizable reads**; followers can lag. After restart `commit_index`
starts at zero and is recovered through leader communication/replication.

## Configuration and durability

YAML configuration contains all members, **including the local node**. Example:

```yaml
node_id: 1
grpc_bind: 0.0.0.0:50051
http_bind: 0.0.0.0:8080
data_dir: data/node-1
heartbeat_interval_ms: 150
election_timeout_min_ms: 750
election_timeout_max_ms: 1500
rpc_timeout_ms: 300
nodes:
  - id: 1
    address: http://node1:50051
  - id: 2
    address: http://node2:50051
  - id: 3
    address: http://node3:50051
```

The timing and bind fields shown above are the defaults. IDs must be unique,
positive integers and peer addresses must be unique HTTP gRPC URLs. Unknown
YAML fields, invalid membership, and invalid timing are rejected at startup.
Choose timeouts well above expected network latency and storage sync latency;
the minimum election timeout must exceed three heartbeat intervals and the RPC
timeout. Timeouts affect availability, while durable voting protects election
safety. Membership and node IDs must match on every node.

CLI overrides: `--node-id` (`RAFT_NODE_ID`), `--data-dir` (`RAFT_DATA_DIR`), and
`--config` (`RAFT_CONFIG_PATH`, default `config.yaml`). Relative data paths are
relative to the process working directory. Each node needs its own durable
directory. `hard-state.json` stores its term, vote, node ID, and member IDs.
Writes sync a temporary file, rename it atomically, then sync the directory
before sending a vote or publishing a changed term. An OS file lock prevents
concurrent use. Corruption, mismatched identity/membership, and persistence
failure stop the node instead of resetting its voting history. Preserve this
directory across restarts, including container replacement.

`log.journal` stores checksummed, length-framed suffix mutations. Appending a
replacement suffix is one frame, so interrupted writes never expose only its
truncation. Each accepted batch is synced before acknowledging AppendEntries;
the leader also syncs its local entries before counting them toward a quorum.
Recovery discards only an incomplete trailing frame and rejects complete corrupt
frames. Existing election-only directories are accepted as empty logs while
preserving their terms and votes. Preserve **both** files; do not edit or remove
durable state. Storage errors stop the node rather than acknowledging unsafe
writes.

Replication reuses connections, bounds requests to 64 entries and approximately
128 KiB, and permits one outstanding RPC per peer. Conflict-term hints accelerate
catch-up. Request/response sizes are capped at 256 KiB. Membership is trusted;
there is no authentication or TLS, so use a private network. The RPC change
replaces Heartbeat rather than supporting rolling upgrades from election-only
nodes: upgrade all nodes together.

Start a Docker cluster with modern Docker Compose v2 or newer:

```bash
NODE_COUNT=3 docker compose up --build -d
docker compose logs -f raft
docker compose port --index 1 raft 8080
# Query the host port returned above, e.g.:
curl http://127.0.0.1:32768/api/status
docker compose down
```

`NODE_COUNT` defaults to 3 and sets `deploy.replicas` as well as the generated
membership. Keep the same value for subsequent Compose commands, or set it in
`.env`. Odd cluster sizes are usually preferred; 3 nodes tolerate 1 failure,
5 tolerate 2. A single node works, too.

The separate `config-generator` container runs
[generate-config.sh](docker/generate-config.sh), writes a shared YAML file, and
prepares data directories for the runtime UID. Nodes start only after it exits
successfully. Compose container names `<project>-raft-<index>` provide individual
peer addresses; each entrypoint matches its IP to these DNS names to determine
its stable node ID. The project name can be changed with `docker compose -p ...`
or `COMPOSE_PROJECT_NAME`.

Each node uses `/data/node-<id>` in the state volume. HTTP is published on a
separate automatically assigned loopback port for every replica, and gRPC stays
on the Compose network. Health checks report whether the REST server responds,
including on a follower or a node without a quorum.

Logging uses `RUST_LOG`, for example `RUST_LOG=raft=debug` or
`RUST_LOG=raft=trace`. `info` reports lifecycle and role/term/leader transitions,
`warn` reports invalid requests and new peer outages, `debug` reports votes and
continued failures, `trace` reports heartbeats and status reads, and `error`
reports fatal failures.

## Tests

```bash
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --all --check
```

Tests cover log matching, current-term majority commitment, stale responses,
single-node operation, interrupted writes/corruption, REST validation, follower
catch-up, leader replacement, divergent suffix repair, and cluster restarts.
