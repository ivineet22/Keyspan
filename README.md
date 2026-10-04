# Keyspan

A few keys can land on one shard and take most of the traffic. Moving them is unsafe if two processes own the same range, or if a crash during the move leaves the range with nobody.

Keyspan is a multi-tenant key-value store that splits that hot slice and moves it to a colder shard. A Go router places each key. Three C++ processes store the bytes. Postgres is the only record of who owns a range, so a crash in the middle still leaves one owner.

```text
client
  -> router (Go)         hash the key, forward, retry once if the epoch is stale
  -> one shard (C++)     append a record, serve from the memory index
Postgres                 ranges, epochs, and the move queue
Redis                    request counts per range
```

There is no replica. If the process that owns a range is dead, that range is down until the same process replays its log. A failover path would hide whether ownership was actually single-valued.

## Storage is a separate process

The router is the API and the ownership cache. It does not hold the values. Each shard is a C++ process because the file is the part this project owns: append a frame, fsync, rebuild the index after a crash, and refuse a key Postgres has assigned to another shard. The two sides share a hash function, not an address space. `acme` / `user-1` hashes to `0xaa7a19bc` in Go and in C++, and the shard checks that vector before it listens.

Placement is FNV-1a over the tenant, a zero byte, and the key, so tenant `ab` + key `c` does not collide with tenant `a` + key `bc`. The 32-bit line is cut into ranges. A range has one owner and an epoch. The epoch starts at 1 and increments only when ownership changes.

## Moving a range

A move is a row in `moves`, not a blocking admin call. A worker leases one row with `FOR UPDATE SKIP LOCKED` and runs one step. The destination opens a TCP stream to the source and pulls records. The worker never sees the values.

The source keeps serving through the snapshot and the catch-up. Puts pause only when the uncopied tail is small. That pause is a fence: a put comes back `503` with body `retry` and is not stored. After the last pull, ownership flips in one statement:

```sql
UPDATE ranges
SET owner_shard = $dest, epoch = epoch + 1
WHERE epoch = $expected
```

A worker that retries and finds the epoch already advanced stops. It does not move the range again. The source deletes its copy only after it has read the new epoch.

The wire format is a 4-byte length and a body. Four calls did not need gRPC.

## Measured

Same generator both times: about 80% of the puts come from one tenant into a narrow hash band.

| | Busiest shard | Share of requests | Busiest / mean |
| --- | --- | --- | --- |
| Fixed placement | shard-1 | 88.5% | 2.65× |
| After rebalance | shard-3 | 41% | 1.23× |

The balancer split the hot range until no shard held most of the traffic. A copy keeps the latest record for each key in the window, not every older put of that key. The worker was killed mid-move and the same row finished. See [results/baseline.txt](results/baseline.txt) and [results/rebalanced.txt](results/rebalanced.txt).

[results/crash.txt](results/crash.txt) is the other check. Destination or source killed during the copy, during the fence, and after commit, and the worker killed after the epoch update. Each case still had one owner and one shard serving the key. A repeated put still read one value.

## Layout

| Path | What it is |
| --- | --- |
| `cmd/server` | HTTP entrypoint and `/metrics` |
| `internal/router` | range cache, forward, Redis `INCR` |
| `internal/worker` | one leased step at a time |
| `internal/balance` | when to split, when to move, when to leave a range alone |
| `internal/wire` | the TCP frames |
| `shard/src` | log, index, recover, fence, pull |
| `results` | the numbers above |

`store_requests_total` and `store_request_duration_seconds` are on `GET /metrics`. A Redis outage does not fail a get.

## Build and test

Windows, Visual Studio 2022, Go, PostgreSQL 17, and Redis on localhost. The shard delay-loads `libpq.dll`. Set `POSTGRES_BIN` if it is not in `C:\Program Files\PostgreSQL\17\bin`.

```powershell
cmake -S shard -B build -G "Visual Studio 17 2022" -A x64
cmake --build build --config Release
go test ./...
```

The tests create the database `store_test` and start `bin/shard.exe` on free ports.

## Run

```powershell
$env:DATABASE_URL = "postgres://postgres:devpass@127.0.0.1:5432/store?sslmode=disable"

bin\shard.exe --name shard-1 --listen 127.0.0.1:50051 --data data\shard-1 --database-url $env:DATABASE_URL
bin\shard.exe --name shard-2 --listen 127.0.0.1:50052 --data data\shard-2 --database-url $env:DATABASE_URL
bin\shard.exe --name shard-3 --listen 127.0.0.1:50053 --data data\shard-3 --database-url $env:DATABASE_URL
go run ./cmd/server
```

`DATABASE_URL`, `ADDR`, `SHARDS`, and `REDIS_ADDR` override the defaults. If Redis is down the router logs that and still serves.

```http
PUT /v1/tenants/{tenant}/keys/{key}
GET /v1/tenants/{tenant}/keys/{key}
GET /metrics
```
