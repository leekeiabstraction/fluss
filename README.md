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

# Option 4: Overlay Table with Offset Reference

## Summary

Create a separate log table that stores only enrichment columns plus a reference to the parent table's offset. At read time, a consumer performs a **co-read** -- fetching from both the parent and overlay tables and merging rows by offset alignment. This is the simplest option to implement, requiring no changes to the storage layer, but it introduces co-read overhead and minor offset/metadata duplication.

## Mechanism

### Schema Declaration

```sql
-- Source table (unchanged)
CREATE TABLE device_logs (
  device_id STRING,
  ip STRING,
  payload STRING,
  ts TIMESTAMP
) WITH ('bucket.num' = '16');

-- Overlay table (enrichment columns only)
CREATE TABLE device_logs_enriched OVERLAY OF device_logs (
  geo_region STRING,
  device_owner STRING,
  risk_score DOUBLE
);
```

The `OVERLAY OF` clause establishes a relationship:
- `device_logs_enriched` inherits the same `bucket.num` and `bucket.key` as `device_logs`
- Each row in the overlay corresponds to a row in the parent by offset
- The overlay table implicitly stores the parent offset as metadata

Alternatively, without DDL changes, the overlay relationship can be purely application-level: the enrichment job writes enrichment columns to a second table, and the consumer knows to co-read both.

### On-Disk Structure

Two independent tables, each with their own storage:

```
{dataDir}/device_logs/{bucketId}/
  00000000000000000000.log          <-- full base data
  00000000000000000000.index
  ...

{dataDir}/device_logs_enriched/{bucketId}/
  00000000000000000000.log          <-- enrichment columns + parent offset metadata
  00000000000000000000.index
  ...
```

### Write Flow

1. **Base writer**: Standard `AppendWriter.append(row)` to `device_logs`. No changes.

2. **Enrichment job**: Reads from `device_logs`, computes enrichment, writes to `device_logs_enriched`:
   ```java
   LogScanner scanner = client.newLogScanner("device_logs");
   AppendWriter writer = client.newAppendWriter("device_logs_enriched");
   
   for (ScanRecord record : scanner.poll(timeout)) {
       InternalRow enriched = enrich(record.getRow());
       // Writer automatically records the parent offset as metadata
       writer.appendWithParentOffset(enriched, record.logOffset());
   }
   ```

3. **Offset alignment**: Because both tables use the same bucket count and bucket key, row N in the overlay's bucket B corresponds to row N in the parent's bucket B. The overlay's own offsets (0, 1, 2, ...) align 1:1 with the parent's offsets, assuming the enrichment job processes every record.

### Read Flow: Co-Read

The consumer opens scanners on both tables and merges by offset:

```
Consumer co-read:

  LogScanner parentScanner  = subscribe(device_logs, bucket, offset)
  LogScanner overlayScanner = subscribe(device_logs_enriched, bucket, offset)

  poll():
    parentRecords  = parentScanner.poll()
    overlayRecords = overlayScanner.poll()
    
    for each (parent, overlay) matched by offset:
      mergedRow = merge(parent.getRow(), overlay.getRow())
      emit mergedRow
```

**System-level co-read** (if Fluss provides it natively):

```
LogScanner mergedScanner = client.newMergedLogScanner("device_logs", "device_logs_enriched");
// Internally manages two scanners, merges by offset, presents unified ScanRecords
```

## Codebase Changes Required

### Minimal (Application-Level Co-Read)

If the co-read is handled by the application or Flink job, **zero changes** to Fluss internals are needed. The enrichment job is just a Flink job that reads from one table and writes to another -- this works today.

### Moderate (System-Level Co-Read)

| Component | File | Change |
|-----------|------|--------|
| New: `MergedLogScanner` | Client library | Wraps two `LogScanner` instances, merges by offset |
| New: `OverlayTableDescriptor` | Metadata | Declares parent-overlay relationship in table metadata |
| `FlinkTableSource` | `fluss-flink/.../source/FlinkTableSource.java` | Support overlay tables as a virtual merged source |
| `FlinkSourceSplitReader` | `fluss-flink/.../source/reader/FlinkSourceSplitReader.java` | Co-read mode: manage two scanners per split |

**No changes to**: `LogTablet`, `LocalLog`, `LogSegment`, `Replica`, `ReplicaManager`, RPC protocol, storage format, or replication.

## Tiered Storage Integration

### Tier-Eligibility

Each table tiers independently. There is no cross-table gate:

```
device_logs segment 0:          tier when rolled (existing behavior)
device_logs_enriched segment 0: tier when rolled (existing behavior)
```

**No enrichment watermark needed for tiering.** Each table manages its own lifecycle. This is the simplest tiering story of all options.

### Remote Format

Standard -- each table uploads its own segments to remote storage. No companion files, no merged uploads.

### Lake Materialization

**This is where the simplicity breaks down.** With two separate tables in the lake:

```
Lake catalog:
  device_logs           (Parquet, base columns)
  device_logs_enriched  (Parquet, enrichment columns)
```

Analytical queries must join these two tables:

```sql
-- Analyst must know about both tables and join them
SELECT d.device_id, d.ip, e.geo_region, e.risk_score
FROM device_logs d
JOIN device_logs_enriched e ON d._offset = e._parent_offset
```

**Mitigations**:
- Create a **lake view** that hides the join: `CREATE VIEW device_logs_complete AS SELECT ... FROM device_logs JOIN device_logs_enriched ...`
- The lake materializer could merge both tables into a single Parquet table (but this adds complexity and reintroduces the duplication problem in the lake)

### Comparison with Other Options

| Aspect | Options 1-3 (same table) | Option 4 (overlay) |
|--------|--------------------------|---------------------|
| Tiering complexity | Must coordinate column groups | Independent, simple |
| Lake tables | Single table, all columns | Two tables, requires join/view |
| Remote read | One fetch | Two fetches (one per table) |

## Failure Mode: Enrichment Falls Behind

### Impact

**Less severe than other options** because there is no cross-table coupling:

- The parent table tiers, ages, and gets consumed independently
- The overlay table lags, but this doesn't block any operation on the parent table
- Disk usage grows only in the overlay's TabletServer allocation (and the parent is unchanged)

There is no cascading failure from enrichment lag to parent table tiering or retention. The tables are decoupled.

### Consumer Impact

The co-read consumer must handle the case where the overlay scanner has fewer records than the parent scanner:

```
Parent scanner at offset: 1500
Overlay scanner at offset: 800 (enrichment lagging)

Co-read behavior:
  offsets 0..800:    merged (all columns available)
  offsets 801..1500: options:
    a) Block until overlay catches up (WAIT)
    b) Emit parent-only rows with NULL enrichment (BEST_EFFORT)
    c) Buffer parent rows until overlay catches up (BUFFERED)
```

These are the same consumer contracts as Option 2, but implemented in the `MergedLogScanner` client rather than the server:

| Mode | Implementation location | Complexity |
|------|------------------------|------------|
| WAIT | `MergedLogScanner`: only emit when both scanners have data | Simple |
| BEST_EFFORT | `MergedLogScanner`: emit parent rows, merge overlay when available | Simple |
| BUFFERED | `MergedLogScanner` or `FlinkSourceSplitReader`: buffer parent, wait for overlay | Medium |

### Operational Metrics

| Metric | Derivation |
|--------|-----------|
| `enrichment_lag` | `parent.latestOffset - overlay.latestOffset` (per bucket) |
| `co_read_null_rate` | Percentage of merged rows with NULL enrichment columns |

### Recovery

Restart the enrichment Flink job. It resumes from its last checkpoint offset in the parent table, writes to the overlay table. No coordination with the parent table needed.

### Retention Coupling

**Critical constraint**: The parent table's retention must be >= the overlay consumer's read horizon. If parent segments are deleted before the overlay consumer reads them, the co-read fails for those offsets.

More specifically:
- The overlay table references parent offsets
- If the parent's retention deletes offset N, but the overlay still has enrichment for offset N, the co-read returns enrichment columns but cannot fetch the parent columns
- The co-reader must handle this: either skip (data loss) or error

**Mitigation**: Set parent retention >= max(overlay lag, consumer lag). Alert when enrichment lag approaches parent retention.

## Consumer Experience

### Application-Level Co-Read

The simplest approach -- no Fluss changes needed. The consumer manages two scanners:

```java
LogScanner parent = client.newLogScanner("device_logs");
LogScanner overlay = client.newLogScanner("device_logs_enriched");
parent.subscribe(bucket, offset);
overlay.subscribe(bucket, offset);

while (true) {
    ScanRecords parentRecords = parent.poll(Duration.ofMillis(100));
    ScanRecords overlayRecords = overlay.poll(Duration.ofMillis(100));
    
    // Merge by offset (both tables have same bucket count, 1:1 offset mapping)
    for (TableBucket tb : parentRecords.buckets()) {
        List<ScanRecord> pRecs = parentRecords.records(tb);
        List<ScanRecord> oRecs = overlayRecords.records(tb);
        // Match by position (offset 0 in parent = offset 0 in overlay)
        for (int i = 0; i < pRecs.size(); i++) {
            if (i < oRecs.size()) {
                emit(merge(pRecs.get(i).getRow(), oRecs.get(i).getRow()));
            } else {
                emit(pRecs.get(i).getRow());  // enrichment not yet available
            }
        }
    }
}
```

**Problems with application-level co-read**:
- Consumer must handle mismatched poll sizes (parent returns more records than overlay)
- Offset alignment assumes 1:1 mapping -- if the enrichment job filters/skips records, alignment breaks
- Two network round-trips per poll
- Checkpoint must track offsets for both tables

### System-Level Co-Read (MergedLogScanner)

A `MergedLogScanner` in the client library hides the complexity:

```java
MergedLogScanner scanner = client.newMergedLogScanner(
    "device_logs", "device_logs_enriched",
    MergeMode.BEST_EFFORT
);
scanner.subscribe(bucket, offset);

for (ScanRecord record : scanner.poll(timeout)) {
    // Unified row: base + enrichment columns (enrichment may be NULL)
    process(record.getRow());
}
```

### Flink Connector

The `FlinkSourceSplitReader` manages two scanners per split:

```
FlinkSourceSplitReader (overlay mode):
  parentScanner:  subscribes to device_logs bucket B
  overlayScanner: subscribes to device_logs_enriched bucket B
  
  fetch():
    poll both scanners
    merge by offset
    return unified ScanRecords
```

Flink SQL could support this via table options:

```sql
SELECT device_id, ip, geo_region, risk_score
FROM device_logs
/*+ OPTIONS('overlay.table'='device_logs_enriched') */
```

## Edge Cases

### Non-1:1 Enrichment

If the enrichment job **filters** records (e.g., only enriches records matching a condition), the 1:1 offset assumption breaks:

```
Parent:  [0] [1] [2] [3] [4] [5]
Overlay: [0] [1] [2]          <-- overlay offset 0 corresponds to parent offset 0
                                   overlay offset 1 corresponds to parent offset 3 (skipped 1,2)
                                   BROKEN: offset alignment lost
```

**Fix**: The overlay must store the **parent offset explicitly** in each record, not rely on positional alignment. This adds a column (or batch-level metadata) to every overlay record -- a small but real duplication cost.

### Schema Evolution

- Adding columns to the parent table: overlay co-read must stay in sync with the parent schema. If the parent adds a column, the overlay's merge logic needs to know about it.
- Adding columns to the overlay: straightforward -- just a new column in the overlay table.
- The parent and overlay schemas must be versioned together if they evolve.

### Multiple Overlays

A parent can have multiple overlays (geo enrichment, fraud scoring, identity resolution):

```
device_logs                 (base)
device_logs_geo             (geo enrichment overlay)
device_logs_fraud           (fraud scoring overlay)
device_logs_identity        (identity resolution overlay)
```

The co-read becomes a multi-way merge. Each additional overlay adds another network round-trip per poll. At 3+ overlays, this becomes a significant performance concern.

## Pros

- **Simplest implementation** -- zero changes to Fluss storage layer, replication, or RPC protocol
- **Works today** -- application-level co-read requires no Fluss changes at all
- **Append-only invariant fully preserved** -- both tables are standard log tables
- **Decoupled lifecycle** -- parent and overlay tier, retain, and fail independently
- **No new system primitives** -- no watermarks, no column groups, no companion files
- **Incremental adoption** -- can start with application-level co-read, later add system-level `MergedLogScanner`

## Cons

- **Co-read overhead** -- every consumer poll requires two (or more) network round-trips. This is per-poll, not one-time.
- **Offset duplication** -- the overlay table has its own offsets (0, 1, 2, ...) distinct from the parent's. If using explicit parent offset references, that's additional per-record storage.
- **Metadata duplication** -- each overlay record has its own timestamp, headers, batch metadata, separate from the parent's.
- **Lake catalog pollution** -- two (or more) tables in the lake instead of one. Analysts must know to join them or use a view.
- **Retention coupling** -- parent retention must exceed overlay consumer's read horizon. No system-level enforcement.
- **Alignment fragility** -- 1:1 offset mapping breaks if enrichment filters/skips records. Explicit parent offset storage adds complexity.
- **Multi-overlay scaling** -- each additional overlay adds another scanner, network round-trip, and merge step.

## When to Choose This Option

- You need enrichment now, with zero changes to Fluss internals
- The enrichment job produces exactly one output per input (1:1 mapping, no filtering)
- There is only one enrichment overlay (not multiple independent enrichments)
- Co-read latency (two network round-trips per poll) is acceptable
- The team prefers operational simplicity over storage/I/O efficiency
- This is a prototype or proof-of-concept, with plans to migrate to a deeper integration (Options 1-3) later
