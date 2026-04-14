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

# Option 3: Segment-Level Column Append

## Summary

Work at the `LogSegment` granularity rather than individual offsets. The enrichment job reads a completed segment's worth of records, computes enrichment columns for the entire segment, and writes a **companion column file** alongside the original segment. The base log is never modified; the companion file is written once, atomically, for a sealed segment.

This is conceptually similar to how Parquet/ORC support adding column chunks, and it parallels how Fluss already handles tiered storage -- a segment is a unit of work that gets post-processed.

## Mechanism

### Schema Declaration

Same as Options 1 and 2:

```sql
CREATE TABLE device_logs (
  device_id STRING,
  ip STRING,
  payload STRING,
  ts TIMESTAMP,
  geo_region STRING COLUMN GROUP 'enriched',
  device_owner STRING COLUMN GROUP 'enriched',
  risk_score DOUBLE COLUMN GROUP 'enriched'
) WITH ('bucket.num' = '16');
```

### Core Idea: Segment as Unit of Enrichment

Instead of enriching individual offsets, enrichment operates on **sealed segments**:

```
Timeline:

1. Base writer fills segment 00000000000000000000.log (offsets 0..1,048,575)
2. Segment rolls -- new segment 00000000000001048576.log starts receiving writes
3. Enrichment job detects segment 0 is sealed
4. Enrichment job reads all records from segment 0, computes enrichment columns
5. Enrichment job writes companion file: 00000000000000000000.enrich.log
   (Arrow column batches for geo_region, device_owner, risk_score, same offset range)
6. LogSegment now has two backing files, merged on read
```

### On-Disk Structure

```
{dataDir}/{tablePath}/{bucketId}/
  00000000000000000000.log                    <-- base segment (sealed, immutable)
  00000000000000000000.index
  00000000000000000000.timeindex
  00000000000000000000.enrich.log             <-- companion enrichment file (written once)
  00000000000000000000.enrich.index           <-- offset index for enrichment columns
  00000000000001048576.log                    <-- active segment (still receiving writes)
  00000000000001048576.index
  00000000000001048576.timeindex
                                              <-- no .enrich files yet (segment not sealed)
```

### Write Flow

1. **Base writer**: Standard append. No changes to the write path.

2. **Segment seal notification**: When a segment rolls, an event is emitted (or the enrichment job polls for sealed segments). This is analogous to how `RemoteLogManager` detects segments ready for upload.

3. **Enrichment job**: Reads the sealed segment end-to-end, computes enrichment columns, writes the companion file atomically:
   - Reads via `LogScanner` or a new `SegmentReader` API
   - Writes the companion file directly (new API) or via a `SegmentColumnAppender`
   - The companion file covers the exact same offset range as the base segment
   - Write is atomic: the file is written to a temp path then renamed

4. **Registration**: `LogTablet` is notified that the companion file exists. It updates `LogSegment` to include the companion in its file set. This can be detected by file presence (convention-based naming) or explicit registration.

### Read Flow

`LogSegment.read()` merges base and companion files:

```
LogSegment.read(fetchOffset, maxBytes, projectedFields):
  |
  +-- Which column groups are requested? (from projectedFields)
  |     |
  |     +-- Only base columns: read from .log file only
  |     +-- Only enrichment columns: read from .enrich.log file only  
  |     +-- Mixed: read from both, merge by offset alignment
  |
  +-- Companion file exists?
  |     |
  |     +-- YES: merge columns from both files
  |     +-- NO: return base columns, NULLs for enrichment columns
  |
  +-- Return merged Arrow batch
```

### Implicit Enrichment Watermark

The enrichment state is implicit in the segment structure:

```
Segment 0: .log   .enrich.log   -> enriched
Segment 1: .log   .enrich.log   -> enriched
Segment 2: .log   .enrich.log   -> NOT enriched (companion not yet written)
Segment 3: .log   (active)       -> NOT enriched (not yet sealed)

Implicit EWM = last offset of segment 1 = 2,097,151
```

This is less precise than Option 2's per-offset EWM (granularity is segment-level, not offset-level), but it's much simpler to implement and reason about.

## Codebase Changes Required

| Component | File | Change |
|-----------|------|--------|
| `LogSegment` | `fluss-server/.../log/LogSegment.java` | Support companion files. Merge on read when companion exists |
| `LogTablet` | `fluss-server/.../log/LogTablet.java` | Detect/register companion files. Expose segment-level enrichment status |
| `LocalLog` | `fluss-server/.../log/LocalLog.java` | Minor: pass companion awareness to `LogSegment` construction |
| New: `SegmentColumnAppender` | Server or client-side | API for writing a companion column file for a sealed segment |
| `FetchLogRequest` | `FlussApi.proto` | No change -- `projected_fields` handles column selection |
| `FetchLogResponse` | `FlussApi.proto` | Optionally: flag indicating whether enrichment columns were available for the returned range |

**Notably smaller change set than Options 1 and 2** -- no changes to `Replica`, `AppendWriter`, `ProduceLogRequest`, replication protocol, or watermark tracking.

## Tiered Storage Integration

### Tier-Eligibility

Natural fit -- the segment is the unit of both enrichment and tiering:

```
tierEligible(segment) =
    segment.isRolled()
    && segment.hasCompanionFile("enriched")   // simple file existence check
```

**Much simpler than Option 2's watermark-based gate.** No watermark arithmetic, no per-offset tracking. Either the companion file exists or it doesn't.

### Remote Format

**Merge on upload (natural choice)**:

When a segment has its companion file, `RemoteLogManager` reads both files, merges the columns, and uploads a single consolidated Arrow file to remote storage. Remote segments are always complete.

```
Local:
  segment-0.log + segment-0.enrich.log
  |
  v (merge on upload)
Remote:
  segment-0.remote.log (all columns merged)
```

### Timeout Escape Valve

Same pattern as Option 2, but simpler to implement:

```
tierPolicy(segment):
  if segment.hasCompanionFile("enriched"):
    upload MERGED                               <- happy path
    
  else if segment.age > ENRICHMENT_WAIT_TIMEOUT:
    upload BASE ONLY                            <- escape valve
    mark INCOMPLETE in RemoteLogManifest
    
  else:
    wait
```

### Late Backfill to Remote Storage

If a segment was tiered without enrichment:

```
Companion file written locally for already-tiered segment
  |
  v
RemoteLogManager detects: segment is remote + INCOMPLETE + companion now exists locally
  |
  v
Upload companion file to remote storage
  |
  v
Update RemoteLogManifest -> COMPLETE
  |
  v
(later) compaction merges into single file
```

### Lake Materialization

Same gating: lake materializer waits for the companion file before writing Parquet. With timeout, writes base-only Parquet, later rewritten on compaction after backfill.

## Failure Mode: Enrichment Falls Behind

### Impact

Same fundamental problem as other options: if the enrichment job can't keep up, unenriched segments accumulate locally.

**Key difference**: The granularity of the problem is segments, not offsets. This makes the situation easier to reason about operationally -- "we have 5 segments awaiting enrichment" is more actionable than "we have 5 million offsets awaiting enrichment."

### Protection Mechanisms

#### 1. Time-Bounded Tier Wait

Same as Option 2. Segments tier with base columns only after timeout.

#### 2. Metrics

| Metric | What it tells you |
|--------|------------------|
| `segments_awaiting_enrichment` | Count of sealed segments without companion files |
| `oldest_unenriched_segment_age` | How stale the oldest unenriched segment is |
| `enrichment_segment_lag` | Number of sealed segments between enrichment position and latest |
| `segment_enrichment_duration` | How long it takes to enrich one segment (throughput signal) |

#### 3. Segment-Level Recovery

Recovery is straightforward: the enrichment job simply processes sealed segments it hasn't enriched yet. No offset tracking, no watermark reconciliation -- just "does segment N have a companion file?"

### Operational Simplicity

```
Enrichment lag alert fires
  |
  +-- How many segments are behind?
  |     +-- Few (1-3): enrichment is slow but catching up
  |     +-- Many (10+): enrichment throughput insufficient, scale up
  |
  +-- Restart enrichment job
  |     It resumes from the first segment without a companion file
  |     No offset tracking needed
```

## Consumer Experience

### The Latency Problem

**This is the primary drawback of Option 3**: enrichment is only available after a segment is sealed, which means enrichment latency is bounded by segment fill time.

```
Segment size: 1 GB (default LOG_SEGMENT_FILE_SIZE)
Write rate: 100 MB/min

Segment fill time: ~10 minutes
+ Enrichment processing time: ~2 minutes

Enrichment latency: ~12 minutes (minimum)
```

For many enrichment use cases (analytics, batch processing, data warehousing), 10-15 minutes is acceptable. For real-time alerting or fraud detection, it is not.

### Consumer Modes

Since enrichment is segment-granular, the consumer modes are simpler:

| Mode | Behavior |
|------|----------|
| `WAIT` | Only read segments that have companion files. Consumer lags by at least one segment fill time. |
| `BEST_EFFORT` | Read all segments. Enrichment columns are NULL for segments without companion files. Transitions to enriched when companion arrives. |

**No redelivery mode**: Since enrichment happens at segment boundaries, and consumers read through segments sequentially, a consumer in `BEST_EFFORT` mode will naturally see enrichment columns become available when it reaches a new segment. There's no need to revisit previously-read offsets -- the consumer just sees NULLs for the current (not-yet-enriched) segment and full columns for older (enriched) segments.

The exception: a consumer currently reading segment N while enrichment completes for segment N. The consumer would see a transition from NULLs to filled columns mid-segment. This can be handled by:
- Not serving enrichment columns for a segment until the companion file is fully written (atomic swap)
- Or accepting the mid-segment transition (simpler, no user-visible issue for most use cases)

### Client Code

```java
// Simple -- NULLs for unenriched segments, filled for enriched ones
for (ScanRecord record : scanner.poll(timeout)) {
    // Enrichment columns are NULL if this record's segment isn't enriched yet
    // They'll be non-NULL once the consumer reaches enriched segments
    process(record.getRow());
}
```

### Flink Connector

No special buffering needed. The `FlinkSourceSplitReader` reads segments as they are. If a segment has a companion file, enrichment columns are merged. If not, they're NULL. The transition is seamless because it happens at segment boundaries.

## Pros

- **Simplest storage-layer change** -- companion files are an additive concept; no modification to existing segment write path
- **Append-only invariant preserved** -- the base log is never modified. The companion file is a new, write-once file alongside the sealed segment.
- **No new watermark concept** -- enrichment status is implicit in file existence
- **Natural fit with tiered storage** -- segments are already the unit of tiering; enrichment companions slot in cleanly
- **Simple recovery** -- enrichment job resumes from first segment without companion file
- **Zero storage duplication, zero offset duplication**
- **Atomic enrichment** -- companion file is written once for an entire segment (temp file + rename), no partial states

## Cons

- **Enrichment latency is segment-bounded** -- minimum latency equals segment fill time (minutes, not seconds). This is a fundamental limitation.
- **Not suitable for real-time enrichment** -- if consumers need enriched data within seconds, this option cannot deliver
- **Segment size tradeoff** -- smaller segments reduce enrichment latency but increase file count, metadata overhead, and tiering frequency. The default 1 GB segment size is optimized for throughput, not enrichment latency.
- **No per-offset enrichment tracking** -- can't express "offsets 0-500 are enriched but 501-1000 are not" within a segment. It's all-or-nothing per segment.
- **Active segment is never enriched** -- the currently active (not-yet-rolled) segment can never have enrichment columns, since it's still receiving writes

## When to Choose This Option

- Enrichment latency of minutes (not seconds) is acceptable
- You want the simplest implementation with the fewest changes to Fluss internals
- The append-only invariant is sacred and cannot be relaxed
- The enrichment job is batch-oriented (processes chunks of data, not individual records)
- You value operational simplicity -- segment-level reasoning is easier than offset-level
- The use case is analytics/data warehousing where completeness matters more than latency
