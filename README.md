# Charm - A charming little key-value store.

Charm is a distributed key-value store.
It is built on top of the Raft consensus algorithm and is written in Rust.

## Features
- Linearizable reads and writes
- Idempotent writes (exactly-once semantics)
- Fault-tolerant for up to `n/2 - 1` failures
- Automated leader election, with requests forwarded to the leader

## Upcoming Features
- [ ] Read-only optimization (Read Index)
- [ ] Snapshotting
- [ ] Joint consensus for cluster membership changes
- [ ] Multi-raft

## Testing Roadmap
- [x] Deterministic simulation
- [x] Crash resilience
- [ ] Network partition resilience

## API
Charm exposes a GRPC API for interacting with the cluster, available in `proto/charm.proto`.

## Running a cluster
See the arguments in `src/bin/charm.rs` for the available options.
