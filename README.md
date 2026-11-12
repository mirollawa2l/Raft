# Lab 2: Raft

This project asks you to implement leader election, heartbeats, and replicated
log entries in `raft/raft.go`.

## Requirements

- Go 1.22 or newer
- No external packages

## Run the tests

Run all tests from the project root:

```text
go test ./...
```

Run the leader-election checkpoint:

```text
go test -run 3A ./raft
```

Run the replicated-log tests:

```text
go test -run 3B ./raft
```

Run the tests with Go's race detector:

```text
go test -race ./...
```

## Scope

Implement the Raft protocol in `raft/raft.go`. Use the supplied `labrpc`
package for all communication between Raft peers.

Do not modify `labrpc`, the supplied tests, or the public Raft method
signatures. You may add private types, fields, methods, and helper goroutines
inside `raft.go`.

Persistence, crash recovery from stable storage, cluster membership changes,
snapshots, and log compaction are outside the scope of Lab 2. The
`UseSnapshot` and `Snapshot` fields in `ApplyMsg` are kept for interface
compatibility and must be ignored.

## Test phases

The 3A tests cover leader election and heartbeats. The 3B tests cover log
replication, commitment, recovery from network partitions, conflict repair,
and RPC volume.
