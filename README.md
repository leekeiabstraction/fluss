<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Option 02: Late-Materialized Columns with Enrichment Watermark

This branch is a working POC of **Option 2** from the enrichment-column design space:
late-materialized columns with a per-column-group Enrichment Watermark (EWM). It is
based on `fluss-0.9.0-incubating` and adds:

- `columnGroup` field on `Schema.Column` (with builder + JSON serde)
- `ColumnGroupStore` and EWM tracking on `LogTablet`
- `ColumnGroupEWMITCase` integration test (5 tests, all passing)

The remainder of this README is the combined design context that motivated the work:

1. [Fluss Log Table Internals](#part-1-fluss-log-table-internals) — how Fluss log
   tables, the client, and Flink union reads work today.
2. [Problem Statement](#part-2-problem-statement) — the data-duplication cost in
   today's enrichment pipelines.
3. [Option 2 Design](#part-3-option-2-late-materialized-columns-with-enrichment-watermark)
   — the late-materialized columns + EWM design implemented here.

---

# Part 1: Fluss Log Table Internals

This document captures how client reading and writing to log tables works in Apache Fluss, including how Flink SQL performs union reads (snapshot + log). It is intended as a reference for future sessions working with the Fluss codebase.

---

## Table of Contents

1. [Overview](#overview)
2. [Key Concepts](#key-concepts)
3. [Writing to Log Tables](#writing-to-log-tables)
4. [Reading from Log Tables](#reading-from-log-tables)
5. [Log Storage Layer](#log-storage-layer)
6. [RPC Protocol](#rpc-protocol)
7. [Flink Union Read](#flink-union-read)
8. [Key File Reference](#key-file-reference)

---

## Overview

Fluss has two table types:

- **Log Tables** -- append-only (no primary key). Only the LogStore is activated.
- **Primary Key Tables** -- support upserts/deletes. Both KvStore (RocksDB) and LogStore are activated; the LogStore holds the changelog.

Data is partitioned into **buckets** (analogous to Kafka partitions). Each bucket is managed by a **LogTablet** on a TabletServer. Buckets are replicated across TabletServers with leader-based replication.

```
Cluster Architecture
====================

  CoordinatorServer                TabletServer 0          TabletServer 1
  +-----------------+              +----------------+      +----------------+
  | Metadata Mgmt   |              | LogTablet B0   |      | LogTablet B0   |
  | Table Manager   |   manages    |   (leader)     |      |   (follower)   |
  | Rebalance Mgr   | ----------> | LogTablet B1   |      | LogTablet B1   |
  | ZK Coordination |              |   (follower)   |      |   (leader)     |
  +-----------------+              +----------------+      +----------------+
                                          ^                       ^
                                          |   Netty RPC           |
                                    +-----+-----------------------+-----+
                                    |         Client (fluss-client)      |
                                    |  AppendWriter / LogScanner / Admin |
                                    +-----------------------------------+
```

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **TableBucket** | Logical unit: `(tableId, partitionId?, bucketId)`. Smallest unit of storage and replication. |
| **LogTablet** | Physical entity managing all log segments for one bucket. Thread-safe wrapper around `LocalLog`. |
| **LocalLog** | Append-only log composed of `LogSegment`s. Not thread-safe; guarded by `LogTablet`'s lock. |
| **LogSegment** | A single segment file (`.log`) plus sparse indexes (`.index`, `.timeindex`). |
| **LogFormat** | `ARROW` (default, columnar) or `INDEXED` (row-oriented). Determines encoding and projection capability. |
| **High Watermark** | The log offset up to which data is replicated to all in-sync replicas. Only committed data (below HWM) is visible to consumers. |
| **Replica** | A copy of a bucket on a TabletServer. One leader handles reads/writes; followers replicate. |

---

## Writing to Log Tables

### End-to-End Write Flow

```
User Application
       |
       v
+------------------+
| AppendWriter     |  Public API: append(InternalRow) -> CompletableFuture<AppendResult>
| (AppendWriterImpl)|
+------------------+
       |  encode row + extract bucket key
       v
+------------------+
| WriterClient     |  Routes record to bucket, manages batching
+------------------+
       |  WriteRecord -> RecordAccumulator
       v
+------------------+
| RecordAccumulator|  Buffers records per (table, bucket) into WriteBatch
+------------------+    Flush triggers: batch size, linger timeout, explicit flush()
       |
       v
+------------------+
| Sender (thread)  |  Background thread drains ready batches
+------------------+
       |  Builds ProduceLogRequest, sends via Netty RPC
       v
=== Network (Netty + Protobuf) ===
       |
       v
+------------------+
| TabletService    |  Server-side RPC handler, authorizes table
+------------------+
       |
       v
+------------------+
| ReplicaManager   |  Routes to correct Replica per TableBucket
+------------------+
       |
       v
+------------------+
| Replica          |  Validates leadership, applies write
+------------------+
       |
       v
+------------------+
| LogTablet        |  synchronized(lock) { appendAsLeader() }
+------------------+    Validates batches, assigns monotonic offsets
       |
       v
+------------------+
| LocalLog         |  Appends to active LogSegment, may roll new segment
+------------------+
       |
       v
+------------------+
| LogSegment       |  Writes to FileLogRecords (.log), updates OffsetIndex + TimeIndex
+------------------+
       |
       v
  ProduceLogResponse(baseOffset per bucket)
       |  propagates back through Sender -> callback -> CompletableFuture
       v
  AppendResult returned to user
```

### Step-by-Step Details

#### 1. Public API -- `AppendWriter`

- **Interface**: `AppendWriter.append(InternalRow)` returns `CompletableFuture<AppendResult>`
- **Implementation**: `AppendWriterImpl`
- **Location**: `fluss-client/.../table/writer/AppendWriter.java`

#### 2. Row Encoding

`AppendWriterImpl` encodes rows based on the table's `LogFormat`:

| Format | Encoder | Use Case |
|--------|---------|----------|
| ARROW (default) | `ArrowWriter` | Columnar batches, supports projection pushdown, compression (LZ4/ZSTD) |
| INDEXED | `IndexedRowEncoder` | Row-oriented, smaller overhead for sparse data |
| COMPACTED | `CompactedRowEncoder` | Compact row format for primary key tables |

A **bucket key** is extracted from the row (if table defines `bucket.key` columns) via `KeyEncoder.ofBucketKeyEncoder()`.

#### 3. Bucket Assignment

`WriterClient` routes each `WriteRecord` to a bucket via `BucketAssigner`:

| Assigner | When Used | Behavior |
|----------|-----------|----------|
| `HashBucketAssigner` | Table has `bucket.key` | Deterministic hash of key columns |
| `StickyBucketAssigner` | Default (no key) | Sticks to one bucket per batch for efficiency |
| `RoundRobinBucketAssigner` | Configured explicitly | Even distribution across buckets |

#### 4. Batching -- `RecordAccumulator`

Records accumulate in per-`(table, bucket)` write batches. Batch types: `IndexedLogWriteBatch`, `ArrowLogWriteBatch`, `CompactedLogWriteBatch`.

Flush triggers:
- **Size**: batch reaches `CLIENT_WRITER_BATCH_SIZE`
- **Time**: linger exceeds `CLIENT_WRITER_BATCH_TIMEOUT`
- **Explicit**: `WriterClient.flush()`

Memory: `LazyMemorySegmentPool` (indexed/compacted) or Arrow's `BufferAllocator`.

#### 5. Sending -- `Sender` Thread

Background thread continuously drains ready batches, builds `ProduceLogRequest` via `ClientRpcMessageUtils.makeProduceLogRequest()`, sends to the correct leader TabletServer via `TabletServerGateway.produceLog()`.

Key configs:
- `CLIENT_WRITER_ACKS`: `"all"` (wait for replication), `"1"` (leader only), `"0"` (fire-and-forget)
- `CLIENT_WRITER_MAX_INFLIGHT_REQUESTS_PER_BUCKET`: flow control
- `CLIENT_WRITER_ENABLE_IDEMPOTENCE`: exactly-once semantics

#### 6. Server-Side Write

1. **TabletService.produceLog()** -- deserializes request, extracts `MemoryLogRecords` per bucket
2. **ReplicaManager.appendRecordsToLog()** -- routes to `Replica` per `TableBucket`
3. **Replica.appendRecordsToLeader()** -- validates leadership
4. **LogTablet.appendAsLeader()** -- core write under `synchronized(lock)`:
   - Validates record batches (CRC, timestamps, writer state for idempotence)
   - Assigns monotonically increasing offsets
   - Appends to active `LogSegment` via `LocalLog.append()`
   - Rolls new segment when: size exceeds `LOG_SEGMENT_FILE_SIZE`, index full, or offset overflow
5. **LogSegment.append()** -- writes `MemoryLogRecords` to `FileLogRecords` (.log file), updates sparse `OffsetIndex` and `TimeIndex`

Response: `ProduceLogResponse` returns `baseOffset` per bucket (first offset assigned to the batch).

---

## Reading from Log Tables

### End-to-End Read Flow

```
User Application
       |
       v
+------------------+
| LogScanner       |  Public API: subscribe(bucket, offset), poll(timeout) -> ScanRecords
| (LogScannerImpl) |
+------------------+
       |
       v
+------------------+
| LogFetcher       |  Builds FetchLogRequest, sends async RPCs per leader server
+------------------+    Supports: projection pushdown, filter pushdown, remote log
       |
       v
=== Network (Netty + Protobuf) ===
       |
       v
+------------------+
| TabletService    |  Server-side: fetchLog()
+------------------+
       |
       v
+------------------+
| ReplicaManager   |  Routes to Replica, builds FetchParams
+------------------+
       |
       v
+------------------+
| LogTablet.read() |  Chooses local vs remote segments
+------------------+
       |
       +---------- Local: OffsetIndex binary search -> FileLogRecords from disk
       |
       +---------- Remote: returns RemoteLogFetchInfo -> client downloads segment
       |
       v
  FetchLogResponse(records, highWatermark)
       |
       v
+------------------+
| CompletedFetch   |  Deserializes LogRecordBatch -> ScanRecord objects
+------------------+    Variants: DefaultCompletedFetch (local), RemoteCompletedFetch (tiered)
       |
       v
+------------------+
| LogFetchBuffer   |  Thread-safe buffer between fetcher and polling thread
+------------------+
       |
       v
+------------------+
| LogFetchCollector|  Drains buffer, respects max-poll-records, packages results
+------------------+
       |
       v
  ScanRecords (Map<TableBucket, List<ScanRecord>>)
       |
       v
  User processes records: record.logOffset(), record.timestamp(), record.getRow()
```

### Step-by-Step Details

#### 1. Public API -- `LogScanner`

- **Location**: `fluss-client/.../scanner/log/LogScanner.java`
- `subscribe(bucket, offset)` -- subscribe to a bucket at a specific offset
- `subscribe(partitionId, bucket, offset)` -- for partitioned tables
- `subscribeFromBeginning(bucket)` -- uses `EARLIEST_OFFSET = -2`
- `poll(Duration timeout)` -- returns `ScanRecords`
- `wakeup()` -- interrupt a blocking poll

#### 2. Offset Discovery

Clients manage offsets manually (no consumer group protocol). Starting offsets are discovered via `BucketOffsetsRetrieverImpl` using the `LIST_OFFSETS` RPC:
- `latestOffsets()` -- current end of log
- `earliestOffsets()` -- start of log
- `offsetsFromTimestamp()` -- binary search by timestamp

#### 3. Fetching -- `LogFetcher`

Groups subscribed buckets by leader TabletServer, builds `FetchLogRequest` per server:

```
FetchLogRequest:
  follower_server_id: -1 (identifies as client, not follower replica)
  max_bytes, min_bytes, max_wait_ms (backpressure / long-poll)
  tables_req[]:
    table_id
    projected_fields[] (column pruning, Arrow format only)
    filter_predicate (server-side pushdown)
    buckets_req[]:
      bucket_id, partition_id?
      fetch_offset (where to start reading)
      max_fetch_bytes (per-bucket limit)
```

Key features:
- **Projection pushdown** -- only fetch specific columns (Arrow format)
- **Filter pushdown** -- server evaluates predicates, skips non-matching batches
- **Long polling** -- `min_bytes` + `max_wait_ms` delays response until enough data
- **Tiered log** -- when data is in remote storage, response contains `RemoteLogFetchInfo`; `RemoteLogDownloader` fetches the segment file locally

#### 4. Buffering and Collection

- `LogFetchBuffer` -- thread-safe queue between fetcher and poll thread; `awaitNotEmpty(deadline)` blocks until data arrives
- `CompletedFetch` -- deserializes `LogRecordBatch` into `ScanRecord` objects (variants: `DefaultCompletedFetch` for direct RPC, `RemoteCompletedFetch` for tiered storage)
- `LogFetchCollector` -- drains buffer, respects `CLIENT_SCANNER_LOG_MAX_POLL_RECORDS`, returns `ScanRecords`

#### 5. `ScanRecord` Contents

Each record contains:
- `logOffset()` -- position in the log
- `timestamp()` -- record timestamp
- `getChangeType()` -- `INSERT`, `DELETE`, `UPDATE_BEFORE`, `UPDATE_AFTER`
- `getRow()` -- the actual data as `InternalRow`
- `getSizeInBytes()` -- for metrics

#### 6. Server-Side Fetch

1. **TabletService.fetchLog()** -- builds `FetchParams` (projection, filters, byte limits)
2. **ReplicaManager.fetchLogRecords()** -- routes to each `Replica`
3. **LogTablet.read()** -- chooses local vs remote:
   - **Local**: `OffsetIndex` binary search finds physical position, reads `FileLogRecords` from `.log` file
   - **Remote**: returns `RemoteLogFetchInfo` pointing to tiered segment
4. Applies projection and filter predicates at log level
5. Long-poll: delays response until `min_bytes` satisfied or `max_wait_ms` expires

Key configs:
- `CLIENT_SCANNER_LOG_FETCH_MAX_BYTES` -- total fetch size limit
- `CLIENT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET` -- per-bucket limit
- `CLIENT_SCANNER_LOG_FETCH_MIN_BYTES` -- minimum to trigger response
- `CLIENT_SCANNER_LOG_FETCH_WAIT_MAX_TIME` -- max long-poll wait
- `CLIENT_SCANNER_LOG_CHECK_CRC` -- CRC validation

---

## Log Storage Layer

### On-Disk Structure

Each bucket's log is stored as a series of segment files:

```
{dataDir}/{tablePath}/{bucketId}/
  00000000000000000000.log          <-- Record data (segment 0)
  00000000000000000000.index        <-- Sparse offset -> position index
  00000000000000000000.timeindex    <-- Sparse timestamp -> offset index
  00000000000001048576.log          <-- Next segment (named by baseOffset)
  00000000000001048576.index
  00000000000001048576.timeindex
  ...
```

### Class Hierarchy

```
LogTablet (thread-safe, per-bucket)
  |
  +-- LocalLog (not thread-safe, guarded by LogTablet.lock)
  |     |
  |     +-- LogSegments (ConcurrentSkipListMap<baseOffset, LogSegment>)
  |           |
  |           +-- LogSegment
  |                 +-- FileLogRecords     (.log file -- record data)
  |                 +-- LazyIndex<OffsetIndex>   (.index -- 8-byte entries: 4B relative offset + 4B position)
  |                 +-- LazyIndex<TimeIndex>     (.timeindex -- 12-byte entries: 8B timestamp + 4B relative offset)
  |
  +-- RemoteLogTablet (tiered segments, if enabled)
  |     +-- RemoteLogManifest (list of RemoteLogSegment)
  |
  +-- WriterStateManager (idempotence tracking)
```

### Segment Rolling

A new segment is created when:
1. Current segment size exceeds `LOG_SEGMENT_FILE_SIZE` (default 1GB)
2. Offset index becomes full
3. Time index becomes full
4. Offset would overflow 32-bit relative offset

### Indexes

- **OffsetIndex** -- sparse mapping of logical offset to physical byte position. 8-byte entries. Binary search for lookups.
- **TimeIndex** -- sparse mapping of timestamp to offset. 12-byte entries. Monotonically increasing timestamps.
- Both use **memory-mapped files** for efficient I/O.
- Lazily initialized (via `LazyIndex`) to avoid memory overhead for inactive segments.
- Rebuilt from the `.log` file on crash recovery (no checksums stored).

### Recovery

- **`recovery-point-offset-checkpoint`** -- stores the flushed offset per bucket; messages beyond this are re-validated on startup
- **`.fluss_cleanshutdown`** marker -- if present, skips recovery; if absent, `LogLoader` scans all segments, rebuilds indexes, and truncates invalid data

### Tiered Storage

When enabled, `RemoteLogManager` uploads old segments to remote storage (S3, HDFS, OSS, Azure, GCS):
- Tracks segments via `RemoteLogManifest` (stored in ZooKeeper)
- `LogTablet` maintains both local and remote offset ranges
- Reads transparently route to the correct tier
- `RemoteStorageCleaner` (on CoordinatorServer) expires old remote segments

---

## RPC Protocol

### Framework

- **Transport**: Netty 4 (TCP, `TCP_NODELAY=true`, `SO_KEEPALIVE=true`)
- **Serialization**: Protocol Buffers v2 (proto2)
- **Proto file**: `fluss-rpc/src/main/proto/FlussApi.proto`

### Wire Format

```
Frame: [FrameLength: int32] [Header] [Payload]

Request Header (8 bytes):
  ApiKey:     int16
  ApiVersion: int16
  RequestId:  int32

Response Header (5 bytes):
  ResponseType: int8
  RequestId:    int32
```

### Key API Keys (from `ApiKeys.java`)

| API Key | ID | Direction | Purpose |
|---------|----|-----------|---------|
| `PRODUCE_LOG` | 1014 | Client -> TabletServer | Write records to log |
| `FETCH_LOG` | 1015 | Client -> TabletServer | Read records from log |
| `LIST_OFFSETS` | 1021 | Client -> TabletServer | Discover offsets (earliest/latest/timestamp) |
| `GET_METADATA` | 1012 | Client -> Any | Discover bucket-to-server mapping |
| `API_VERSIONS` | 0 | Client -> Any | Negotiate supported API versions |

### Metadata Discovery and Routing

- `MetadataUpdater` sends `GetMetadata` RPCs to discover which TabletServer leads each bucket
- Results cached in `Cluster` (immutable snapshot of server topology)
- Refreshed on errors or leadership changes
- `GatewayClientProxy` uses Java reflection proxies to intercept gateway method calls and route them via `RpcClient`

---

## Flink Union Read

"Union read" is the pattern where Flink reads a **bounded snapshot** (historical data) then seamlessly transitions to **streaming log consumption** (real-time changes), providing a complete, gap-free view.

### When Each Mode Applies

```
Decision Tree for Split Type
=============================

Is lake tiering enabled?
  |
  +-- YES --> LakeSplitGenerator creates splits
  |             |
  |             +-- Lake snapshot exists?
  |                   |
  |                   +-- YES --> LakeSnapshotAndFlussLogSplit (lake files + Fluss log)
  |                   +-- NO  --> LogSplit (pure log streaming)
  |
  +-- NO --> Is it a primary key table with FULL startup mode?
               |
               +-- YES --> KV snapshot exists for bucket?
               |             |
               |             +-- YES --> HybridSnapshotLogSplit (KV snapshot + log)
               |             +-- NO  --> LogSplit (pure log streaming)
               |
               +-- NO --> LogSplit (pure log streaming)
```

| Table Type | Lake? | Split Type | Behavior |
|------------|-------|------------|----------|
| Log table (no PK) | No | `LogSplit` | Pure log streaming, no union read |
| Primary key table | No | `HybridSnapshotLogSplit` | KV snapshot then log changelog |
| Any table | Yes | `LakeSnapshotAndFlussLogSplit` | Lake snapshot files then Fluss log |

### Variant 1: Primary Key Table -- KV Snapshot + Log (HybridSnapshotLogSplit)

This is the core union read pattern.

#### Architecture

```
                         JobManager
                    +-------------------+
                    | FlinkSource-      |
                    | Enumerator        |
                    +-------------------+
                            |
          1. Admin.getLatestKvSnapshots(tablePath)
          2. For each bucket:
             snapshotId + logOffset from KvSnapshots
          3. Create HybridSnapshotLogSplit(snapshotId, logStartingOffset)
             or LogSplit(offset) if no snapshot
                            |
              assign splits to readers
                            |
            +---------------+---------------+
            |                               |
     TaskManager 0                   TaskManager 1
  +-------------------+           +-------------------+
  | FlinkSource-      |           | FlinkSource-      |
  | SplitReader       |           | SplitReader       |
  +-------------------+           +-------------------+
  | Phase 1: Snapshot |           | Phase 1: Snapshot |
  |  BatchScanner     |           |  BatchScanner     |
  |  (bounded)        |           |  (bounded)        |
  +-------------------+           +-------------------+
  | Phase 2: Log      |           | Phase 2: Log      |
  |  LogScanner       |           |  LogScanner       |
  |  (streaming)      |           |  (streaming)      |
  +-------------------+           +-------------------+
```

#### Enumerator: Split Creation

In `FlinkSourceEnumerator.getSnapshotAndLogSplits()` (line 657):

```
For each bucket in KvSnapshots:
  |
  +-- snapshotId present?
        |
        +-- YES: logOffset = snapshots.getLogOffset(bucketId)
        |        create HybridSnapshotLogSplit(tb, partitionName, snapshotId, logOffset)
        |
        +-- NO:  get offset from startingOffsetsInitializer
                 create LogSplit(tb, partitionName, startingOffset)
```

The **critical invariant**: `logStartingOffset` is co-determined with the snapshot. It is the log position at the time the KV snapshot was taken. This guarantees no gap or overlap between phases.

#### Reader: Split Handling

In `FlinkSourceSplitReader.handleSplitsChanges()` (line 200):

```
Receive HybridSnapshotLogSplit:
  |
  +-- If snapshot NOT finished:
  |     Queue in boundedSplits (for BatchScanner reading)
  |
  +-- ALWAYS subscribe LogScanner at logStartingOffset
       (log data starts buffering immediately in background)
```

#### Reader: fetch() Control Flow

```
FlinkSourceSplitReader.fetch()
  |
  +-- removedSplits not empty? --> return removed split IDs
  |
  +-- checkSnapshotSplitOrStartNext()
  |     Dequeue next bounded split, create BatchScanner
  |
  +-- currentBoundedSplitReader != null?
  |     |
  |     +-- YES: Read batch from BatchScanner
  |     |          |
  |     |          +-- records returned? --> return as bounded split records
  |     |          +-- null (exhausted)? --> finishCurrentBoundedSplit()
  |     |                                     HybridSnapshotLogSplit: NOT marked finished
  |     |                                     (split stays alive for log phase)
  |     |
  |     +-- NO: Poll from logScanner
  |              return as log records with split IDs
```

#### Record Emitter: Phase-Aware State Tracking

In `FlinkRecordEmitter.emitRecord()` (line 54):

```
Is HybridSnapshotLogSplitState?
  |
  +-- scanRecord.logOffset() >= 0?
  |     |
  |     +-- YES (log phase):
  |     |     hybridState.setNextOffset(logOffset + 1)
  |     |     (also sets snapshotFinished = true)
  |     |
  |     +-- NO (snapshot phase, offset < 0):
  |           hybridState.setRecordsToSkip(readRecordsCount)
  |
  +-- Deserialize ScanRecord -> OUT via FlussDeserializationSchema
  +-- sourceOutput.collect(record, timestamp)
```

#### Checkpoint and Recovery

On checkpoint:
- **Snapshot phase**: `HybridSnapshotLogSplitState` stores `recordsToSkip` count. On recovery, `BoundedSplitReader` skips that many records before emitting.
- **Log phase**: stores `nextOffset`. On recovery, `LogScanner` subscribes at that offset.
- **Lease management**: `FinishedKvSnapshotConsumeEvent` is sent to the enumerator when snapshot phase completes. The enumerator releases the KV snapshot lease after the next successful checkpoint.

### Variant 2: Lake Snapshot + Fluss Log (LakeSnapshotAndFlussLogSplit)

When lake tiering is enabled (Iceberg, Paimon, Lance):

```
FlinkSourceEnumerator
  |
  +-- generateHybridLakeFlussSplits()
        |
        +-- LakeSplitGenerator queries lake catalog for snapshot files
        +-- Creates LakeSnapshotAndFlussLogSplit:
              lakeSnapshotSplits: list of lake data files (Parquet/ORC)
              startingOffset: Fluss log offset to resume from
        |
        v
FlinkSourceSplitReader
  |
  +-- Phase 1: Read lake files via LakeSplitReaderGenerator
  |     (bounded, reads Parquet/ORC files)
  |
  +-- Phase 2: Subscribe LogScanner at startingOffset
        (streaming, reads Fluss changelog)
```

The flow is analogous to KV snapshot union read, but reads from external lake storage instead of Fluss KV snapshots. Lake splits are preserved even if the corresponding Fluss partition has expired -- this ensures union reads correctly cover partitions that exist only in the lake.

### Variant 3: Pure Log (No Union Read)

For log tables without lake tiering:

```
FlinkSourceEnumerator.getLogSplit()
  |
  +-- For each bucket: create LogSplit(tableBucket, partitionName, startingOffset)
        |
        v
FlinkSourceSplitReader
  |
  +-- subscribeLog() on LogScanner
  +-- fetch() polls LogScanner directly
  +-- No bounded phase
```

---

## Key File Reference

### Client -- Write Path

| File | Purpose |
|------|---------|
| `fluss-client/.../table/writer/AppendWriter.java` | Public API interface |
| `fluss-client/.../table/writer/AppendWriterImpl.java` | Row encoding and send |
| `fluss-client/.../write/WriterClient.java` | Batching, buffering, bucket assignment |
| `fluss-client/.../write/RecordAccumulator.java` | Per-(table,bucket) batch accumulation |
| `fluss-client/.../write/Sender.java` | Background thread, RPC send |
| `fluss-client/.../write/WriteRecord.java` | Internal record representation |
| `fluss-client/.../write/BucketAssigner.java` | Bucket routing interface |
| `fluss-client/.../utils/ClientRpcMessageUtils.java` | Builds ProduceLogRequest |

### Client -- Read Path

| File | Purpose |
|------|---------|
| `fluss-client/.../scanner/log/LogScanner.java` | Public API interface |
| `fluss-client/.../scanner/log/LogScannerImpl.java` | Implementation with thread-safety guards |
| `fluss-client/.../scanner/log/LogFetcher.java` | Network layer, builds FetchLogRequest |
| `fluss-client/.../scanner/log/LogFetchBuffer.java` | Thread-safe buffer between fetcher and poll |
| `fluss-client/.../scanner/log/CompletedFetch.java` | Deserializes LogRecordBatch -> ScanRecord |
| `fluss-client/.../scanner/log/LogFetchCollector.java` | Drains buffer, packages ScanRecords |
| `fluss-client/.../scanner/log/LogScannerStatus.java` | Per-bucket offset tracking |
| `fluss-client/.../scanner/log/ScanRecords.java` | Result container: Map<TableBucket, List<ScanRecord>> |
| `fluss-client/.../scanner/ScanRecord.java` | Individual record: offset, timestamp, changeType, row |

### Server -- Storage

| File | Purpose |
|------|---------|
| `fluss-server/.../log/LogTablet.java` | Per-bucket log management (thread-safe) |
| `fluss-server/.../log/LocalLog.java` | Append-only segment collection |
| `fluss-server/.../log/LogSegment.java` | Single segment with data + indexes |
| `fluss-server/.../log/LogSegments.java` | ConcurrentSkipListMap of segments |
| `fluss-server/.../log/OffsetIndex.java` | Sparse offset -> position index |
| `fluss-server/.../log/TimeIndex.java` | Sparse timestamp -> offset index |
| `fluss-server/.../log/LogManager.java` | Factory and lifecycle for all LogTablets |
| `fluss-server/.../log/LogLoader.java` | Crash recovery, index rebuild |
| `fluss-server/.../log/remote/RemoteLogManager.java` | Tiered storage upload/download |
| `fluss-server/.../log/remote/RemoteLogTablet.java` | Remote segment tracking |

### Server -- Request Handling

| File | Purpose |
|------|---------|
| `fluss-server/.../tablet/TabletService.java` | RPC handler: produceLog(), fetchLog() |
| `fluss-server/.../replica/ReplicaManager.java` | Routes to Replica, manages replication |
| `fluss-server/.../replica/Replica.java` | Per-bucket replica, entry point for read/write |

### RPC Layer

| File | Purpose |
|------|---------|
| `fluss-rpc/src/main/proto/FlussApi.proto` | Protobuf message definitions |
| `fluss-rpc/.../protocol/ApiKeys.java` | API key enum (PRODUCE_LOG=1014, FETCH_LOG=1015, etc.) |
| `fluss-rpc/.../gateway/TabletServerGateway.java` | RPC interface: produceLog(), fetchLog() |
| `fluss-rpc/.../netty/server/NettyServerHandler.java` | Server-side request parsing |
| `fluss-rpc/.../protocol/MessageCodec.java` | Header encoding/decoding |

### Flink Connector -- Union Read

| File | Purpose |
|------|---------|
| `fluss-flink/.../source/FlinkTableSource.java` | DynamicTableSource impl, projection/filter pushdown |
| `fluss-flink/.../source/FlinkSource.java` | Flink Source<> impl, creates enumerator and reader |
| `fluss-flink/.../source/enumerator/FlinkSourceEnumerator.java` | Split creation: HybridSnapshotLogSplit / LogSplit / LakeSnapshotAndFlussLogSplit |
| `fluss-flink/.../source/reader/FlinkSourceReader.java` | Source reader, state snapshots, event handling |
| `fluss-flink/.../source/reader/FlinkSourceSplitReader.java` | Core read logic: bounded splits then log polling |
| `fluss-flink/.../source/reader/BoundedSplitReader.java` | Reads snapshot batches via BatchScanner |
| `fluss-flink/.../source/emitter/FlinkRecordEmitter.java` | Phase-aware emission and state updates |
| `fluss-flink/.../source/split/HybridSnapshotLogSplit.java` | Split type: snapshotId + logStartingOffset + isSnapshotFinished |
| `fluss-flink/.../source/split/LogSplit.java` | Split type: startingOffset + optional stoppingOffset |
| `fluss-flink/.../source/split/HybridSnapshotLogSplitState.java` | Mutable state: recordsToSkip (snapshot) / nextOffset (log) |
| `fluss-flink/.../lake/split/LakeSnapshotAndFlussLogSplit.java` | Split type: lake files + Fluss log offset |
| `fluss-flink/.../lake/LakeSplitGenerator.java` | Generates hybrid lake + Fluss splits |
| `fluss-flink/.../catalog/FlinkTableFactory.java` | Creates FlinkTableSource from catalog |

---

# Part 2: Problem Statement

## Reducing Data Duplication in Fluss Enrichment Pipelines

A common data enrichment pattern using Fluss log tables looks like:

```
edge device -> log table A -> Flink enrichment job -> log table B -> consumer
```

The enrichment job reads from log table A, performs lookups or transformations (e.g., geo-location from IP, user profile from user ID, fraud scoring), and writes the enriched result to log table B.

**The duplication problem**: Log table B contains all original columns from A plus the enrichment columns. If the original row has 50 columns and the enrichment adds 3, the 50 original columns are stored twice -- once in A and once in B. At scale, this doubles storage and I/O costs.

### What We're Optimizing For

1. **Storage** -- avoid storing the same column data in two tables
2. **I/O** -- avoid writing and reading redundant columns through the network and disk
3. **Operational simplicity** -- avoid maintaining two parallel tables in the lake catalog
4. **Correctness** -- consumers should see a complete, enriched row without application-level joins

### Constraints From Fluss Architecture

Key properties of Fluss log tables (see Part 1 above for full details):

- **Append-only** -- log tables have no primary key; data is written sequentially with monotonically increasing offsets
- **Bucket-partitioned** -- data is split into buckets (analogous to Kafka partitions), each managed by a `LogTablet` on a `TabletServer`
- **Arrow-columnar by default** -- the default `LogFormat.ARROW` stores data in columnar batches, which naturally supports projection pushdown and column-level operations
- **High watermark (HWM)** -- only data replicated to all in-sync replicas is visible to consumers
- **Tiered storage** -- old segments are uploaded to remote storage (S3/HDFS/etc.) by `RemoteLogManager`, tracked via `RemoteLogManifest`
- **Lake materialization** -- data can be materialized to Iceberg/Paimon/Lance for analytical queries

### Options Explored

| Option | Approach | Storage Duplication | Offset Duplication | Impl Complexity |
|--------|----------|--------------------|--------------------|-----------------|
| Option 1 | Column Groups (write at existing offsets) | None | None | Medium |
| **Option 2** | **Late-Materialized Columns + Enrichment Watermark** | **None** | **None** | **High** |
| Option 3 | Segment-Level Column Append | None | None | Low-Medium |
| Option 4 | Overlay Table with Offset Reference | Minimal (offset metadata) | Yes | Low |

**Recommendation**: Option 2 (Late-Materialized Columns with Enrichment Watermark) provides the strongest design, but each option represents a valid point in the complexity/capability tradeoff space. This branch implements Option 2.

---

# Part 3: Option 2 -- Late-Materialized Columns with Enrichment Watermark

## Summary

Same-table column groups (like Option 1), but with a formal **Enrichment Watermark (EWM)** that tracks enrichment progress per column group. Enrichment columns are declared as **nullable-until-filled**. The EWM becomes a system-level primitive that gates consumer reads, tiered storage uploads, and lake materialization -- giving the system an explicit, composable notion of "column completeness."

## Mechanism

### Schema Declaration

```sql
CREATE TABLE device_logs (
  device_id STRING,
  ip STRING,
  payload STRING,
  ts TIMESTAMP,
  -- Enrichment columns, written later
  geo_region STRING COLUMN GROUP 'enriched',
  device_owner STRING COLUMN GROUP 'enriched',
  risk_score DOUBLE COLUMN GROUP 'enriched'
) WITH ('bucket.num' = '16');
```

### The Enrichment Watermark (EWM)

The EWM is a per-bucket, per-column-group offset that represents: "all offsets up to and including this value have enrichment columns filled."

```
Base log:        [0] [1] [2] [3] [4] [5] [6] ...
                                          ^
                                          HWM (high watermark -- replication)

Enrichment log:  [0] [1] [2] [3]
                              ^
                              EWM (enrichment watermark)

Fully available: offsets 0..3 (all columns filled)
Partially available: offsets 4..5 (base columns only, enrichment NULL)
Not yet visible: offset 6+ (not yet replicated)
```

The EWM parallels the existing High Watermark (HWM):
- HWM answers: "up to which offset is data replicated?"
- EWM answers: "up to which offset is enrichment complete?"

Multiple column groups have independent EWMs. The **effective enrichment watermark** is `min(EWM_group1, EWM_group2, ...)`.

### On-Disk Structure

Same as Option 1 -- separate segment files per column group:

```
{dataDir}/{tablePath}/{bucketId}/
  00000000000000000000.log                    <-- default column group
  00000000000000000000.index
  00000000000000000000.timeindex
  00000000000000000000.col.enriched.log       <-- 'enriched' column group
  00000000000000000000.col.enriched.index
```

Plus EWM tracking:
- EWM is maintained in memory by `LogTablet` (or `Replica`)
- Persisted in a checkpoint file (analogous to `recovery-point-offset-checkpoint`)
- Replicated to followers alongside HWM

### Write Flow

1. **Base writer**: Same as standard append. Offsets assigned by `LogTablet.appendAsLeader()`.

2. **Enrichment writer**: Calls `AppendWriter.appendColumns("enriched", sourceOffset, row)`.
   - `LogTablet` validates offset exists in base group
   - Writes enrichment columns to the column group's segment file
   - **Advances EWM** if this write fills a contiguous range from the previous EWM

3. **EWM advancement**: The EWM advances only when enrichment is **contiguous** from the current EWM. If offset 5 is enriched but offset 4 is not, the EWM stays at 3. This ensures the EWM is a reliable "everything up to here is complete" guarantee.

### Read Flow

Same as Option 1 (merge column groups by offset), but the consumer can choose how the EWM affects visibility.

## Consumer Contracts

Three consumption modes that determine how clients interact with incomplete enrichment:

### Contract A: Best-Effort (Fire-and-Forget)

**Each offset is seen exactly once. NULLs in enrichment columns are permanent from the consumer's perspective.**

```
scan.enrichment.mode = best-effort

Consumer reads offset 1000 -> (device_id="abc", geo=NULL)   # enrichment not yet filled
Consumer reads offset 1001 -> ...
# offset 1000 enrichment arrives later -- consumer never sees it
```

- Consumer sees records up to HWM, regardless of EWM
- Enrichment columns are NULL where `offset > EWM`
- No redelivery, no buffering
- Client must handle NULLs in business logic

**Client code:**
```java
for (ScanRecord record : scanner.poll(timeout)) {
    String geo = record.getRow().isNullAt(geoIdx) ? "UNKNOWN" : record.getRow().getString(geoIdx);
    // proceed with possibly-null enrichment
}
```

**Good for**: Metrics pipelines, monitoring, alerting -- where approximate data is acceptable and low latency matters.

**Edge cases**:
- Consumer has no way to know if NULL means "enrichment pending" vs "genuinely no data" -- may need a metadata flag or sentinel value
- If enrichment job is permanently down, all enrichment columns are permanently NULL

### Contract B: Redelivery (Changelog Semantics)

**When enrichment backfill arrives, the system emits an UPDATE for previously-consumed offsets.**

```
scan.enrichment.mode = redelivery

t=1  Consumer reads: offset 1000, INSERT  (device_id="abc", geo=NULL)
t=2  Enrichment fills offset 1000
t=3  Consumer reads: offset 1000, UPDATE_AFTER (device_id="abc", geo="US-WEST")
```

This reuses the existing `ChangeType` enum in `ScanRecord` (`INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE`).

**How redelivery is triggered**: The consumer tracks `lastCompleteEWM`. When EWM advances past it, the consumer re-reads the newly enriched offset range:

```
Consumer state:
  currentOffset = 1500          (latest offset consumed)
  lastCompleteEWM = 800         (EWM when we last checked)

EWM advances from 800 -> 1200:
  -> re-read offsets [801, 1200] with all columns
  -> emit as UPDATE_AFTER records
  -> update lastCompleteEWM = 1200
  -> continue from currentOffset = 1500
```

**Client code:**
```java
for (ScanRecord record : scanner.poll(timeout)) {
    switch (record.getChangeType()) {
        case INSERT:
            downstream.insert(record.getRow());    // first time, enrichment may be NULL
            break;
        case UPDATE_AFTER:
            downstream.upsert(record.getLogOffset(), record.getRow());  // enrichment backfill
            break;
    }
}
```

**Good for**: Databases, KV stores, search indexes -- any downstream that supports upserts.

**Edge cases**:
- **Downstream must support upserts**. Append-only sinks (files, Kafka topics, email notifications) cannot handle UPDATE_AFTER.
- **Offset ordering is violated** during redelivery -- the consumer sees offset 801 (redelivery) after offset 1500 (current). Downstream must not assume offset ordering.
- **Checkpoint complexity**: Must persist both `currentOffset` and `lastCompleteEWM` to recover correctly after failure.
- **Duplicate processing**: If the consumer crashes between processing a redelivered record and checkpointing, it may reprocess the same UPDATE_AFTER on restart. Downstream must be idempotent.

### Contract C: Connector-Managed Buffer (Flink-Native)

**The Flink connector buffers records internally, emitting only when enrichment is available (or timeout expires).**

```
scan.enrichment.mode = buffered    (default for Flink connector)
scan.enrichment.timeout = 30s
scan.enrichment.buffer-size = 100mb
```

The complexity is contained within `FlinkSourceSplitReader`:

```
FlinkSourceSplitReader enhanced behavior:

  Internal state per split:
    baseOffset = 1500           (latest base offset fetched)
    enrichedOffset = 800        (latest offset where enrichment confirmed)
    buffer: TreeMap<offset, Row>

  fetch():
    1. Fetch new base records [baseOffset..HWM], store in buffer with enrichment NULL
    2. Check EWM. For buffered records where offset <= EWM:
         fetch enrichment columns, merge into row, emit as complete record
    3. Buffer too large or oldest record age > timeout?
         emit oldest records with NULL enrichment (graceful degradation)
    4. Return emitted records
```

```
Visualization:

                     EWM          HWM
                      |            |
  ────────────────────┼────────────┼──────
  enriched records    | buffered   | not yet
  (emitted, complete) | (held)     | fetched
```

**Flink SQL usage:**
```sql
SELECT device_id, ip, geo_region, risk_score
FROM device_logs
/*+ OPTIONS('scan.enrichment.mode'='buffered',
            'scan.enrichment.timeout'='30s',
            'scan.enrichment.buffer-size'='100mb') */
```

**Good for**: Most Flink jobs -- provides the cleanest semantics without leaking complexity to user code.

**Edge cases**:
- **Memory pressure**: Buffer size is bounded, but under sustained enrichment lag, the buffer fills and records degrade to NULLs. The `scan.enrichment.buffer-size` must be tuned relative to expected lag.
- **Checkpoint size**: Buffered records must be included in Flink checkpoints. Large buffers increase checkpoint size and duration.
- **Latency**: Records are held until enrichment arrives or timeout expires. For latency-sensitive jobs, the timeout must be short.
- **Non-Flink clients**: This mode is Flink-specific. Raw `LogScanner` clients must use Contract A or B.

### Consumer Contract Comparison

| Dimension | A: Best-Effort | B: Redelivery | C: Buffered |
|-----------|---------------|---------------|-------------|
| Client complexity | Low (handle NULLs) | Medium (handle UPDATE) | None (connector hides it) |
| Downstream requirements | Must tolerate NULLs | Must support upsert | No special requirement |
| Data completeness | Best-effort | Eventually complete | Complete up to buffer limit |
| Offset ordering | Preserved | Violated (re-reads old offsets) | Preserved |
| State overhead | None | EWM tracking | Buffer memory |
| Works for non-Flink clients | Yes | Yes | No |
| Latency | Lowest | Low + redelivery spike | Bounded by timeout |

### Recommended Defaults

| Client type | Default mode | Rationale |
|-------------|-------------|-----------|
| Fluss Java client (`LogScanner`) | `BEST_EFFORT` | Raw clients are expected to handle edge cases |
| Flink connector | `BUFFERED` | Cleanest semantics, connector already manages internal buffering |
| Flink SQL (ad-hoc) | `BEST_EFFORT` | Interactive queries shouldn't block on enrichment |

## Codebase Changes Required

| Component | File | Change |
|-----------|------|--------|
| `LogTablet` | `fluss-server/.../log/LogTablet.java` | Track per-column-group EWM. New `enrichmentWatermark` alongside existing `highWatermark`. Column-group-aware append |
| `Replica` | `fluss-server/.../replica/Replica.java` | EWM replication (followers track it alongside HWM) |
| `LocalLog` | `fluss-server/.../log/LocalLog.java` | Manage parallel segment chains per column group |
| `LogSegment` | `fluss-server/.../log/LogSegment.java` | Multiple `FileLogRecords` per column group. Merge-on-read |
| `AppendWriter` | `fluss-client/.../table/writer/AppendWriter.java` | New: `appendColumns(columnGroup, sourceOffset, row)` |
| `LogScanner` | `fluss-client/.../scanner/log/LogScanner.java` | Enrichment mode config. EWM-aware fetch logic |
| `LogFetcher` | `fluss-client/.../scanner/log/LogFetcher.java` | Pass enrichment mode in `FetchLogRequest` |
| `FlinkSourceSplitReader` | `fluss-flink/.../source/reader/FlinkSourceSplitReader.java` | Buffer mode: hold records until EWM catches up or timeout |
| `ProduceLogRequest` | `FlussApi.proto` | New field: `column_group` (string), `source_offset` (int64) |
| `FetchLogRequest` | `FlussApi.proto` | New field: `enrichment_watermark_mode` enum |
| `FetchLogResponse` | `FlussApi.proto` | New field: `enrichment_watermark` (int64) per column group |
| Checkpoint file | Storage layer | Persist EWM per bucket per column group |

## Tiered Storage Integration

### Tier-Eligibility Gate

A segment is tier-eligible only when all column groups are filled for its offset range:

```
tierEligible(segment) =
    segment.isRolled()
    && for all columnGroups g:
         enrichmentWatermark(g) >= segment.lastOffset()
```

**This means remote segments are always "complete"** -- all columns filled. No sparse reads from remote storage.

### Enrichment Lag: The Escape Valve

If enrichment falls behind, the tier-eligibility gate blocks segment uploads, causing local disk to fill. To prevent cascading failure, a **time-bounded wait** acts as an escape valve:

```
tierPolicy(segment):
  if allColumnGroupsFilled(segment):
    upload MERGED (all columns)              <- happy path

  else if segment.age > ENRICHMENT_WAIT_TIMEOUT:
    upload BASE ONLY                         <- escape valve
    mark segment as INCOMPLETE in RemoteLogManifest

  else:
    wait                                     <- normal backpressure
```

Config: `log.tiering.enrichment-wait-timeout` (e.g., 30 minutes).

### Remote Segment Format

| Scenario | Format | Why |
|----------|--------|-----|
| All groups filled on time | Single merged Arrow file | One read, optimal |
| Timeout: base only uploaded | Base file only | Escape valve |
| Backfill arrives later | Base + companion enrichment file | No rewrite of base |
| Background compaction | Merge into single file | Optimization |

### Late Enrichment Backfill to Remote Storage

When enrichment catches up after a segment was tiered incomplete:

```
Enrichment job reaches offset range of segment [3M, 4M]
  |
  v
Enrichment columns computed, written locally
  |
  v
RemoteLogManager detects: segment [3M, 4M] is remote + INCOMPLETE
  |
  v
Upload enrichment column file as companion:
  /table/bucket-0/
    segment-0000003000000.remote.log                <- base (already there)
    segment-0000003000000.remote.col.enriched.log   <- backfilled
  |
  v
Update RemoteLogManifest:
  { offsetRange: [3M, 4M],
    columnGroups: ["base", "enriched"],
    enrichmentStatus: COMPLETE }
```

### Lake Materialization

The EWM gates lake writes just like tiering:

```
lakeMaterializeEligible(offsetRange) =
    for all columnGroups g:
      enrichmentWatermark(g) >= offsetRange.lastOffset
```

With the timeout escape valve:
- Lake files written after timeout contain only base columns (enrichment NULLs)
- After backfill + lake compaction (Iceberg/Paimon), the Parquet files get the enrichment columns merged in
- **Result**: the lake catalog has a single table with all columns, no "raw" vs "enriched" table split

## Failure Mode: Enrichment Falls Behind

### The Cascading Failure (Without Escape Valve)

```
Base writer:       1 GB/min
Enrichment job:    0.8 GB/min (20% slower, or stalled)

Segment 3: base ✓  enriched ✗  -> tier-eligible ✗  -> STUCK locally
Segment 4: base ✓  enriched ✗  -> STUCK
...
Segment N: disk full -> TabletServer crash
```

### Protection Mechanisms

#### 1. Time-Bounded Tier Wait (described above)

Segments tier with base columns only after `enrichment-wait-timeout`. Local disk is freed.

#### 2. Consumer Degradation

| Consumer mode | Behavior under lag |
|---------------|-------------------|
| `STRICT` | Blocks. Consumer stops advancing. Backpressure propagates upstream. |
| `BEST_EFFORT` | Continues. Enrichment columns are NULL. No impact on throughput. |
| `REDELIVERY` | Continues with NULLs, then redelivers when backfill arrives. |
| `BUFFERED` | Holds records up to buffer limit / timeout, then degrades to NULLs. |

#### 3. Operational Metrics

| Metric | What it tells you |
|--------|------------------|
| `enrichment_lag = HWM - EWM` per group | How far behind enrichment is |
| `segments_awaiting_enrichment` | How many segments are blocked from tiering |
| `segments_timed_out_partial` | How often the escape valve fires |
| `backfill_pending_segments` | How many remote segments need enrichment backfill |
| `enrichment_fill_rate` vs `base_append_rate` | Will enrichment keep up? (trend) |

#### 4. Alert Thresholds

```
WARNING:  enrichment_lag > 0.5 * enrichment_wait_timeout
CRITICAL: enrichment_lag > 0.8 * enrichment_wait_timeout
INFO:     segments_timed_out_partial > 0
```

#### 5. Retention Interaction

If enrichment lag exceeds local retention:
- The segment is eligible for local deletion (retention rule), but enrichment hasn't arrived
- Deleting the local segment means enrichment data can never be written
- System should emit `enrichment_lag_exceeds_retention` warning
- Configurable behavior: hold segments beyond retention (with a hard cap), or delete and accept data loss for enrichment columns

### Recovery Sequence

```
Enrichment lag alert fires
  |
  +-- Is the enrichment Flink job running?
  |     +-- No  -> restart, backfill happens automatically
  |     +-- Yes -> backpressured? Scale up parallelism / check lookup service
  |
  +-- Are segments timing out?
  |     +-- Acceptable (enrichment is best-effort for this use case)
  |     +-- Not acceptable -> increase timeout, add local disk
  |
  +-- Is backfill keeping up with new tiered segments?
        +-- Yes -> system is self-healing
        +-- No  -> enrichment can't sustain base throughput, architectural intervention needed
```

## Full Tiering Lifecycle

```
Segment sealed (rolled)
  |
  +-- All column groups filled? (EWM >= segment.lastOffset)
  |     |
  |     YES -> upload merged -> mark COMPLETE -> local deletable
  |
  |     NO -> age > enrichment-wait-timeout?
  |           |
  |           NO  -> wait (next tiering cycle)
  |           |
  |           YES -> upload base only -> mark INCOMPLETE -> local deletable
  |                   |
  |                   (later) enrichment backfill arrives
  |                   |
  |                   upload companion enrichment file -> mark COMPLETE
  |                   |
  |                   (later) background compaction merges into single file
```

## The EWM as a Universal Gate

| Decision point | Gate condition | Effect |
|----------------|---------------|--------|
| Consumer read (STRICT mode) | `offset <= min(HWM, EWM_*)` | Consumer sees only fully-enriched rows |
| Consumer read (BUFFERED mode) | Buffer + timeout | Records held until enriched or timeout |
| Remote tiering | `segment.lastOffset <= min(EWM_*)` | Remote segments are always complete (with timeout escape) |
| Lake materialization | `offsetRange.last <= min(EWM_*)` | Lake tables have all columns (with timeout escape) |
| Local segment deletion | Segment tiered (which requires EWM gate) | Can't lose un-enriched data (unless retention forces it) |

## Pros

- **Zero storage duplication** -- same table, same offsets, enrichment stored separately
- **Zero offset duplication** -- enrichment writes at existing offsets
- **Enrichment watermark is a powerful primitive** -- one concept gates consumers, tiering, and lake materialization
- **Composable** -- multiple enrichment jobs fill different column groups independently, each with its own EWM: `readable_offset = min(HWM, EWM_geo, EWM_fraud, EWM_identity)`
- **Arrow-native** -- columnar batches naturally support column-subset storage and merging
- **Consumer choice** -- clients declare their consistency/latency tradeoff explicitly
- **Graceful degradation** -- timeout escape valves prevent cascading failures
- **Single lake table** -- no "raw" vs "enriched" table split in the catalog

## Cons

- **Highest implementation complexity** -- new watermark concept, checkpoint persistence, replication, consumer modes, tiering logic
- **Breaks append-only invariant** -- like Option 1, writing at existing offsets is a fundamental change
- **EWM replication overhead** -- followers must track EWM per column group, adding to replication protocol
- **Backfill complexity** -- late enrichment of already-tiered segments requires companion file uploads and manifest updates
- **Consumer mode proliferation** -- four modes (STRICT, BEST_EFFORT, REDELIVERY, BUFFERED) is a large API surface to test and document
- **Retention vs enrichment tension** -- requires careful policy when enrichment lag approaches retention, with no perfect answer

## When to Choose This Option

- Enrichment completeness matters to consumers and must be tracked at the infrastructure level
- Multiple independent enrichment jobs enrich the same base stream
- You need consumers to have explicit control over the completeness/latency tradeoff
- The team is willing to invest in a new system primitive (EWM) that will be maintained long-term
- Lake/tiered storage is in use and must contain enriched data

---

# What This Branch Implements

This branch is an MVP that implements the foundational pieces of Option 2:

| Piece | Status |
|-------|--------|
| Schema column-group metadata (`Schema.Column.columnGroup`) | ✅ Implemented |
| JSON serde round-trip for `column_group` | ✅ Implemented |
| File path helpers for `.col.<group>.log` segments | ✅ Implemented |
| `ColumnGroupStore` (in-memory enrichment + EWM) | ✅ Implemented |
| `LogTablet.registerColumnGroupIfAbsent / putEnrichment / advanceEnrichmentWatermark` | ✅ Implemented |
| `ColumnGroupEWMITCase` integration test (5 tests, all passing) | ✅ Implemented |
| `PRODUCE_LOG_COLUMNS` RPC + `appendColumns` client API | ⏳ Future work |
| Server-side merge-on-read (rebuild Arrow batches) | ⏳ Future work |
| EWM replication to followers | ⏳ Future work |
| Tiering / lake EWM gate | ⏳ Future work |
| `REDELIVERY` and `BUFFERED` consumer modes | ⏳ Future work |

## Running the Integration Test

```bash
mvn verify -pl fluss-client -am \
  -Dit.test=ColumnGroupEWMITCase \
  -Dtest=ColumnGroupEWMITCase \
  -DfailIfNoTests=false
```

Expected: `Tests run: 5, Failures: 0, Errors: 0, Skipped: 0`.
