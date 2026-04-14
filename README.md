# Fluss Log Table Internals: Client Read, Write, and Flink Union Read

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
---

# Reducing Data Duplication in Fluss Enrichment Pipelines

## Problem Statement

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

Key properties of Fluss log tables (see `fluss-log-table-internals.md` for full details):

- **Append-only** -- log tables have no primary key; data is written sequentially with monotonically increasing offsets
- **Bucket-partitioned** -- data is split into buckets (analogous to Kafka partitions), each managed by a `LogTablet` on a `TabletServer`
- **Arrow-columnar by default** -- the default `LogFormat.ARROW` stores data in columnar batches, which naturally supports projection pushdown and column-level operations
- **High watermark (HWM)** -- only data replicated to all in-sync replicas is visible to consumers
- **Tiered storage** -- old segments are uploaded to remote storage (S3/HDFS/etc.) by `RemoteLogManager`, tracked via `RemoteLogManifest`
- **Lake materialization** -- data can be materialized to Iceberg/Paimon/Lance for analytical queries

### Options Explored

| Option | Approach | Storage Duplication | Offset Duplication | Impl Complexity |
|--------|----------|--------------------|--------------------|-----------------|
| [Option 1](./01-column-groups.md) | Column Groups (write at existing offsets) | None | None | Medium |
| [Option 2](./02-late-materialized-columns.md) | Late-Materialized Columns + Enrichment Watermark | None | None | High |
| [Option 3](./03-segment-column-append.md) | Segment-Level Column Append | None | None | Low-Medium |
| [Option 4](./04-overlay-table.md) | Overlay Table with Offset Reference | Minimal (offset metadata) | Yes | Low |

**Recommendation**: Option 2 (Late-Materialized Columns with Enrichment Watermark) provides the strongest design, but each option represents a valid point in the complexity/capability tradeoff space. See individual option files for detailed analysis.

---
---

# Option 1: Column Groups (Write at Existing Offsets)

## Summary

Allow a second writer to append **new columns** to existing offsets in the same log table, stored as separate column segment files on disk. The table schema defines all columns up front, but columns are grouped into **column groups** -- different producers write different groups independently.

## Mechanism

### Schema Declaration

```sql
CREATE TABLE device_logs (
  -- Base columns (written by edge device ingestion)
  device_id STRING,
  ip STRING,
  payload STRING,
  ts TIMESTAMP,
  
  -- Enrichment columns (written later by enrichment job)
  geo_region STRING COLUMN GROUP 'enriched',
  device_owner STRING COLUMN GROUP 'enriched',
  risk_score DOUBLE COLUMN GROUP 'enriched'
) WITH ('bucket.num' = '16');
```

Columns without an explicit `COLUMN GROUP` belong to the implicit `default` group.

### On-Disk Structure

Each column group gets its own segment files, sharing the same offset space:

```
{dataDir}/{tablePath}/{bucketId}/
  00000000000000000000.log                    <-- default column group
  00000000000000000000.index
  00000000000000000000.timeindex
  00000000000000000000.col.enriched.log       <-- 'enriched' column group
  00000000000000000000.col.enriched.index     <-- sparse index for enrichment offsets
```

### Write Flow

1. **Base writer** (ingestion): Calls `AppendWriter.append(row)` as normal. Only default-group columns are populated. Records get assigned offsets 0, 1, 2, ... by `LogTablet.appendAsLeader()`

2. **Enrichment writer**: Calls a new API `AppendWriter.appendColumns("enriched", sourceOffset, row)` where:
   - `"enriched"` is the target column group
   - `sourceOffset` is the base offset this enrichment corresponds to
   - `row` contains only the enrichment columns

3. `LogTablet` validates that the source offset exists in the base group, then writes the enrichment columns to the column group's segment file at that offset

### Read Flow

`LogTablet.read()` merges column groups by offset alignment before returning:

```
FetchLogRequest with projected_fields = [device_id, geo_region]
  |
  v
LogTablet.read():
  1. Read device_id from default group's segment
  2. Read geo_region from enriched group's segment  
  3. Merge by offset
  4. Return merged Arrow batch
```

The existing `projected_fields` in `FetchLogRequest` naturally selects from merged column groups -- a consumer requesting only base columns never touches the enrichment segment files.

## Codebase Changes Required

| Component | File | Change |
|-----------|------|--------|
| `LogSegment` | `fluss-server/.../log/LogSegment.java` | Hold multiple `FileLogRecords` -- one per column group. Add merge-on-read logic |
| `LogTablet` | `fluss-server/.../log/LogTablet.java` | `appendAsLeader()` accepts writes tagged with a column group. Validate offset exists in base group |
| `LocalLog` | `fluss-server/.../log/LocalLog.java` | Manage parallel segment chains (one per column group) |
| `AppendWriter` | `fluss-client/.../table/writer/AppendWriter.java` | New method: `appendColumns(columnGroup, sourceOffset, row)` |
| `ArrowWriter` | Encoding layer | Write only columns belonging to target group |
| `ProduceLogRequest` | `FlussApi.proto` | New field: `column_group` (string, optional) |
| `FetchLogRequest` | `FlussApi.proto` | No change -- `projected_fields` already handles column selection |

## Tiered Storage Integration

### Tier-Eligibility

A segment can only be tiered when all column groups have data for that segment's offset range. Without this gate, remote segments would be incomplete.

```
tierEligible(segment) =
    segment.isRolled()
    && for all columnGroups g:
         g.hasDataForRange(segment.baseOffset, segment.lastOffset)
```

**Problem**: There is no explicit watermark tracking enrichment progress. The system must infer completeness by checking whether the enrichment column group's segment files cover the same offset range -- this is fragile and requires scanning the enrichment segment index.

### Remote Format

Two options:

1. **Merge on upload**: Read all column groups locally, merge into single Arrow file, upload. Remote reads are simple but lose the column-group separation.
2. **Upload separately**: Each column group uploaded as its own remote file. Preserves separation but remote reads may need multiple fetches.

### Lake Materialization

Lake writes (Iceberg/Paimon) should only include rows where all column groups are filled. Without an explicit watermark, the lake materializer must check enrichment coverage per offset range -- adding latency and complexity to the lake write path.

## Failure Modes and Edge Cases

### Enrichment Falls Behind

**Impact**: Without an explicit watermark mechanism, there is no clean way to express "enrichment is lagging." The system has no built-in signal for when enrichment data is expected but missing.

**Detection**: Must be inferred by comparing the base log's latest offset against the enrichment column group's latest offset. This is an operational metric, not a system primitive.

**Recovery**: If the enrichment job falls behind:
- Tiering is blocked (segments can't be uploaded incomplete)
- Local disk usage grows
- No automatic escape valve -- an operator must decide whether to tier incomplete segments manually or wait

**Mitigation**: Could add a timeout-based escape valve (similar to Option 2), but without a formal watermark, the logic is ad-hoc.

### Offset Validation

The enrichment writer must write at exact offsets that exist in the base group. This means:
- The enrichment job must track which base offsets it has processed
- Out-of-order enrichment writes are possible (offset 100 enriched before offset 50)
- Sparse enrichment (not every offset enriched) creates gaps -- reads at unenriched offsets return NULLs for enrichment columns

### Concurrent Writers

Multiple enrichment jobs writing to different column groups of the same table can conflict:
- Lock contention on `LogTablet.lock` increases
- Segment rolling for different column groups may happen at different times, complicating alignment

### Consumer Experience

Consumers reading from a table with column groups face the same question as Option 2: what happens when enrichment columns are not yet filled?

- **No built-in watermark**: The consumer cannot ask "give me only fully-enriched records." They must handle NULLs in enrichment columns themselves.
- **No redelivery**: If a consumer reads offset N with NULL enrichment and later the enrichment arrives, the consumer has moved on. There is no mechanism to revisit.
- **Projection helps**: A consumer that only needs base columns is completely unaffected -- projection pushdown avoids touching enrichment segment files entirely.

## Pros

- **Zero storage duplication** -- same table, same offsets, enrichment columns stored separately
- **Zero offset duplication** -- enrichment writes reference existing offsets
- **Projection pushdown works naturally** -- `projected_fields` selects from any column group
- **Familiar mental model** -- similar to HBase/Cassandra column families
- **Simpler than Option 2** -- no new watermark concept

## Cons

- **Breaks append-only invariant** -- writing at existing offsets is fundamentally different from appending. This is a significant change to the `LogTablet` contract.
- **No enrichment watermark** -- no first-class way to express or track enrichment progress. Consumers, tiering, and lake materialization must all work around this gap.
- **Offset validation overhead** -- every enrichment write must verify the target offset exists
- **Segment alignment complexity** -- column groups may roll segments at different times
- **No consumer-side contract** -- consumers cannot choose between "wait for enrichment" and "read immediately with NULLs" -- they always get NULLs if enrichment hasn't arrived

## When to Choose This Option

- The enrichment job keeps up reliably (enrichment lag is not a concern)
- Consumer applications can tolerate NULL enrichment columns
- You want the simplest storage-layer change without introducing new system primitives (watermarks)
- The team is comfortable with the append-only invariant being relaxed for column-group writes
