# Raft elections in Rust

An election-only Raft node using Tokio, tonic/prost gRPC, Axum, and
`log`/`env_logger`. The cluster has fixed membership. Each node starts as a
follower, uses a randomized election timeout, votes once per term, and becomes
leader after receiving a strict majority of the configured members' votes.

The gRPC service has `RequestVote` and a small `Heartbeat` RPC. The latter keeps
followers from starting new elections and reports the leader ID. It carries no
log information.

Run locally with Rust 1.88 or newer (a vendored `protoc` is used automatically):

```bash
cargo build --locked
# Run each command in a separate terminal.
RUST_LOG=info ./target/debug/raft --config config/node1.yaml
RUST_LOG=info ./target/debug/raft --config config/node2.yaml
RUST_LOG=info ./target/debug/raft --config config/node3.yaml
curl http://127.0.0.1:8081/api/status
```

The single REST endpoint, `GET /api/status`, returns a local snapshot:

```json
{
    "node_id": 1,
    "role": "follower",
    "current_term": 4,
    "leader_id": 2
}
```

Roles are `follower`, `candidate`, or `leader`. `leader_id` is `null` until the
node wins an election or hears a current leader's heartbeat. This status is
local knowledge: during a partition an isolated old leader can still report
`leader` while the majority elects a leader in a higher term.

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
