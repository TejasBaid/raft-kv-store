#!/usr/bin/env bash
# Launch a 3-node cluster as separate OS processes (so you can `kill -9` one).
#
#   scripts/cluster.sh            # start nodes on 127.0.0.1:7001-7003, data in ./data
#   scripts/cluster.sh stop       # stop them
#
# Logs go to ./data/node-N.log; PIDs to ./data/node-N.pid.
set -euo pipefail

cd "$(dirname "$0")/.."
DATA=${DATA:-./data}
BASE_PORT=${BASE_PORT:-7001}
BIN=./target/release/raft-kv
PEERS="1=127.0.0.1:${BASE_PORT},2=127.0.0.1:$((BASE_PORT + 1)),3=127.0.0.1:$((BASE_PORT + 2))"

if [[ "${1:-start}" == "stop" ]]; then
  for id in 1 2 3; do
    if [[ -f "$DATA/node-$id.pid" ]]; then
      kill "$(cat "$DATA/node-$id.pid")" 2>/dev/null || true
      rm -f "$DATA/node-$id.pid"
    fi
  done
  echo "stopped"
  exit 0
fi

cargo build --release --quiet --bin raft-kv
mkdir -p "$DATA"
for id in 1 2 3; do
  "$BIN" server --id "$id" --peers "$PEERS" --data-dir "$DATA/node-$id" \
    >"$DATA/node-$id.log" 2>&1 &
  echo $! >"$DATA/node-$id.pid"
  echo "node $id: pid $(cat "$DATA/node-$id.pid"), log $DATA/node-$id.log"
done
sleep 1
"$BIN" status --cluster "127.0.0.1:${BASE_PORT},127.0.0.1:$((BASE_PORT + 1)),127.0.0.1:$((BASE_PORT + 2))"
