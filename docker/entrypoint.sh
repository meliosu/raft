#!/usr/bin/env bash
set -euo pipefail

# Without Compose discovery settings, behave as a normal raft binary entrypoint.
if [[ -z ${NODE_NAME_PREFIX:-} ]]; then
    exec raft "$@"
fi
count=${NODE_COUNT:?NODE_COUNT is required}
if [[ ! $count =~ ^[1-9][0-9]*$ ]] || (( ${#count} > 4 || count > 1000 )); then
    echo "Invalid NODE_COUNT" >&2
    exit 1
fi

# Compose has no replica-index environment variable. Match our container IP to
# the DNS names Compose assigns: <project>-raft-<index>. No Docker socket needed.
read -ra own_ips <<< "$(hostname -i)"
for (( attempt=0; attempt<30; attempt++ )); do
    for (( id=1; id<=count; id++ )); do
        while read -r address _; do
            for own_ip in "${own_ips[@]}"; do
                if [[ $address == "$own_ip" ]]; then
                    echo "Starting node $id (${NODE_NAME_PREFIX}-${id})"
                    exec raft "$@" --node-id "$id" --data-dir "/data/node-$id"
                fi
            done
        done < <(getent ahostsv4 "${NODE_NAME_PREFIX}-${id}" || true)
    done
    sleep 1
done
echo "Could not identify this replica using Compose DNS; use modern Docker Compose v2 container names" >&2
exit 1
