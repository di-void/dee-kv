# dee-kv

`dee-kv` is a small, distributed key-value store built in Rust. It exposes gRPC APIs for data access and runs a simple Raft-style consensus layer for replication.

## Features
- gRPC Store API: Get, Put, Delete
- gRPC Health API: Ping
- Consensus service for leader election and log replication
- Local or Docker-based cluster configuration

## Prerequisites
- Rust toolchain (edition 2024)
- `protoc` (Protocol Buffers compiler) for build-time codegen
- Optional: Docker + Docker Compose for containerized runs

## Quickstart (Local)
This repo includes a sample local cluster config at `DATA/cluster.config.json`.

1) Build and run node 1
```bash
cargo run -- --id=1 --config=./DATA/cluster.config.json
```

2) (Optional) Run a second node in another terminal
```bash
cargo run -- --id=2 --config=./DATA/cluster.config.json
```

By default, nodes bind to loopback addresses. You can override the network interface with `NET_INTERFACE` (see Environment Variables).

## Quickstart (Docker Compose)
Docker compose spins up 3 nodes using `DATA/docker-cluster.config.json`.

```bash
docker compose up --build
```

Ports mapped:
- `node1`: host `50051` -> container `9000`
- `node2`: host `50052` -> container `9000`
- `node3`: host `50053` -> container `9000`

## Configuration
The node requires two CLI args:
- `--id=<node-id>`: must match a node in the config file
- `--config=<path>`: path to a JSON cluster config

Example config (local):
```json
{
  "cluster_name": "dee-kv-demo",
  "nodes": [
    { "id": 1, "address": "127.0.0.1:50051" },
    { "id": 2, "address": "127.0.0.1:50052" }
  ]
}
```

## Environment Variables
- `NET_INTERFACE`:
  - `loopback` (default) uses `127.0.0.1`
  - `wildcard` uses `0.0.0.0` (used in Docker)
- `RUST_LOG`: standard Rust log filter (e.g. `info,dee_kv=debug`)

## gRPC Services
Proto files are in `proto/`:
- `store.proto`: StoreService (Get, Put, Delete)
- `health.proto`: HealthCheckService (Ping)
- `consensus.proto`: ConsensusService (RequestVote, AppendEntries)

## Data & Logs
On-disk state and logs live in `./DATA` by default.

## Development
Typecheck:
```bash
cargo check
```
