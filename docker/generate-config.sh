#!/usr/bin/env bash
set -euo pipefail

count=${NODE_COUNT:-3}
prefix=${NODE_NAME_PREFIX:-raft-raft}
config_dir=${CONFIG_DIR:-/config}
data_dir=${DATA_DIR:-/data}
if [[ ! $count =~ ^[1-9][0-9]*$ ]] || (( ${#count} > 4 || count > 1000 )); then
    echo "NODE_COUNT must be an integer between 1 and 1000" >&2
    exit 1
fi
if [[ ! $prefix =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
    echo "NODE_NAME_PREFIX must be a Docker Compose project/service prefix" >&2
    exit 1
fi
mkdir -p "$config_dir" "$data_dir"
temporary="$config_dir/cluster.yaml.tmp"
trap 'rm -f "$temporary"' EXIT
cat > "$temporary" <<'YAML'
grpc_bind: 0.0.0.0:50051
http_bind: 0.0.0.0:8080
data_dir: /data
heartbeat_interval_ms: 150
election_timeout_min_ms: 750
election_timeout_max_ms: 1500
rpc_timeout_ms: 300
nodes:
YAML
for (( id=1; id<=count; id++ )); do
    printf '  - id: %s\n    address: "http://%s-%s:50051"\n' "$id" "$prefix" "$id" >> "$temporary"
    mkdir -p "$data_dir/node-$id"
    # Runtime containers run as uid 10001. Allow local script tests as a regular user.
    if (( EUID == 0 )); then chown 10001:10001 "$data_dir/node-$id"; fi
done
chmod 644 "$temporary"
mv "$temporary" "$config_dir/cluster.yaml"
echo "Generated config for $count nodes with prefix $prefix"
