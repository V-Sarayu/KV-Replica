# KVReplica

A distributed key-value store with leader-follower replication, written in Go.

Built as a learning exercise to understand core distributed systems concepts —
replication, write-ahead logging, and eventual consistency — without the
complexity of consensus protocols.

## What it does

- A **leader** node accepts client `PUT`/`GET` requests over TCP.
- One or more **follower** nodes replicate every write from the leader in real time.
- Every node keeps its state in an in-memory map, durably backed by a
  **write-ahead log (WAL)** — a local append-only file that's replayed on
  startup to rebuild state after a restart.
- If a follower disconnects and reconnects, it **catches up automatically**
  via a full-state snapshot from the leader (last-write-wins by timestamp).


## Architecture

```
kvreplica/
├── main.go          # entry point — parses -mode flag, starts leader or follower
├── node.go          # shared Node struct: owns Store + WAL, TCP send/receive helpers
├── leader.go         # accepts client + follower connections, broadcasts writes
├── follower.go        # dials leader, syncs snapshot, applies replicated writes
├── store.go          # in-memory map + mutex (Get/Put/Delete/Snapshot)
├── wal.go           # append-only WAL file, replay-on-startup
├── protocol.go        # shared message types sent as JSON over TCP
└── client/
    └── client.go       # CLI client (one-shot commands or interactive REPL)
```

### How a write flows

1. Client sends `PUT key value` to the leader over TCP.
2. Leader appends the write to its own WAL (disk), fsyncs, then applies it
   to its in-memory store.
3. Leader broadcasts the write (with its original timestamp) to every
   currently-connected follower.
4. Each follower appends the write to its own WAL, then applies it to its
   own in-memory store.
5. Leader responds to the client with an ACK.

### How a follower catches up after downtime

1. Follower reconnects and sends `REGISTER_FOLLOWER`.
2. Leader replies with a full snapshot of its current key-value state.
3. Follower **overwrites** its in-memory store with that snapshot — this is
   the "full-state resync" simplification instead of incremental log
   shipping, acceptable for small datasets.
4. Follower resumes receiving live replicated writes.

## Requirements

- Go 1.21 or later (no external dependencies — standard library only)

Check your version:
```bash
go version
```

## Setup

```bash
git clone <this-repo>
cd kvreplica
go build ./...   # sanity check — should complete with no output
```

## Running the demo

Open **4 terminals**, all in the project root.

**Terminal 1 — Leader:**
```bash
go run . -mode=leader -addr=:9000 -wal=leader.wal
```

**Terminal 2 — Follower 1:**
```bash
go run . -mode=follower -leader=localhost:9000 -wal=follower1.wal
```

**Terminal 3 — Follower 2:**
```bash
go run . -mode=follower -leader=localhost:9000 -wal=follower2.wal
```

**Terminal 4 — Client:**
```bash
go run ./client -leader=localhost:9000
```
Then at the `>` prompt:
```
PUT alpha 1
PUT beta 2
GET alpha
```

You should see replication log lines appear in Terminals 1–3 as you `PUT`.

Or run one-shot commands instead of the REPL:
```bash
go run ./client -leader=localhost:9000 PUT alpha 1
go run ./client -leader=localhost:9000 GET alpha
```

### Demo: kill a follower, write more, watch it catch up

1. In Terminal 2, hit `Ctrl+C` to kill follower 1.
2. In Terminal 4, write more keys while it's down:
   ```
   PUT gamma 3
   PUT delta 4
   ```
3. Restart follower 1 with the **same WAL path** so it can replay its
   pre-crash state first:
   ```bash
   go run . -mode=follower -leader=localhost:9000 -wal=follower1.wal
   ```
4. Watch the log: `replayed N WAL entries from follower1.wal` (recovering
   pre-crash state), followed by `synced snapshot, M keys loaded`
   (catching up to the leader via snapshot).

### Verifying state matches across nodes

```bash
cat leader.wal
cat follower1.wal
cat follower2.wal
```

`follower1.wal` may contain a couple of stale pre-crash entries that were
superseded by the snapshot resync — harmless, since the WAL is a durability
log rather than something queried directly for current state.

## CLI flags

| Flag | Applies to | Default | Meaning |
|---|---|---|---|
| `-mode` | all | *(required)* | `leader` or `follower` |
| `-addr` | leader | `:9000` | Address the leader listens on |
| `-leader` | follower | `localhost:9000` | Leader address to connect to |
| `-wal` | all | `<mode>.wal` | Path to this node's WAL file |
| `-leader` | client | `localhost:9000` | Leader address to send commands to |

**Important:** each follower needs its own distinct `-wal=` path — running
two followers with the same WAL file will cause them to corrupt/overwrite
each other's log.

## Known limitations

- Only the leader serves `GET` requests — followers don't expose a
  client-facing read API in this version.
- No leader election: if the leader process dies, the system stops
  accepting writes until it's manually restarted.
- Follower catch-up is a full snapshot, not an incremental diff — fine at
  small scale, would not scale to large datasets.
- A follower's WAL can retain stale entries after a snapshot resync,
  since resync overwrites in-memory state without pruning the log.