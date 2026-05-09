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

# Phase E: Enrichment Replication & Leader Failover

**Status:** Design — not implemented
**Last updated:** 2026-05-09
**Builds on:** Phases A–D (most recent: `8c0d8cf5`, `a04a16d0`)

## 1. Context

Phases A–D shipped column-group enrichment as a **leader-only** feature:

- The leader of a bucket persists `.col.<group>.log` (FileLogRecords) and
  `.col.<group>.index` (OffsetIndex) per (bucket, group) via
  `EnrichmentSegment` — see `fluss-server/src/main/java/org/apache/fluss/server/log/EnrichmentSegment.java`.
- `LogTablet.appendColumnsAsLeader` advances a per-group Enrichment Watermark
  (EWM = last source offset + 1) — see `fluss-server/.../log/LogTablet.java:321-340`.
- The read-side EWM gate caps fetches that project enrichment columns at the
  leader's local EWM — see `Replica.computeEffectiveEnrichmentCap`,
  `fluss-server/.../replica/Replica.java:1671-1702`.
- Same-node recovery walks the bucket directory and seeds EWM from each index's
  last entry — `LogTablet.discoverEnrichmentSegments`, `LogTablet.java:472-508`.

**The gap.** None of this is replicated. On leader failover:

- The follower has no enrichment files on disk.
- The new leader's EWM resets to -1 for every group.
- Clients projecting enrichment columns suddenly observe no rows.

This document specifies how enrichment data is replicated alongside the base
log, how a "committed" enrichment watermark is defined and propagated, and how
the read-side gate switches from leader-local EWM to a committed value that
survives clean failover.

## 2. Goals & non-goals

### Goals

- Enrichment writes are durable across **clean** leader failover.
- A client that observes data at offset N continues to observe it across
  failovers — no read regression in the steady state.
- Reuse the existing follower-fetch pipeline; do not introduce a parallel
  replication channel.
- Backwards-compatible wire and on-disk format for tables without column groups.

### Non-goals

- **Unclean leader election preserves CEW.** Out-of-sync followers winning
  election is the same data-loss case as base log; resolved alongside the
  planned leader-epoch cache (`ReplicaFetcherThread.java:603-604`).
- **Multi-segment enrichment lifecycle.** Phase C ships a single segment per
  (bucket, group) at `baseOffset=0`; rolling/retention/tiering is Tier 3.
- **Cross-region or async replication.** ISR-based synchronous only.

## 3. Architecture

### 3.1 New state

| State | Location | Scope |
|---|---|---|
| `committedEnrichmentWatermarks: Map<String, Long>` | `LogTablet` | Per-group, on every replica. CEW = `min(EWM_g across ISR)`. |
| `followerEwmByGroup: Map<String, Map<Integer, Long>>` | `Replica` (leader-only) | Per-group, per-follower latest acknowledged EWM. Drives CEW computation. |
| Per-(group) fetch cursor | Follower's `RemoteLeaderEndpoint` request builder | Advertised in each fetch request so the leader knows what enrichment to send. |

### 3.2 Flow

```
Producer ── PRODUCE_LOG_COLUMNS ──▶ Leader (bucket B)
                                     │ (1) appendColumnsAsLeader → persist + bump local EWM_g

Followers ◀─── FETCH_LOG (resp:  ──── Leader
                base + enrichment    │ (2) Read enrichment in [follower_ewm, leader_ewm)
                + CEW)               │
   │                                 │
   │ (3) Persist enrichment          │
   │ (4) Bump follower-local EWM_g   │
   │ (5) Adopt CEW from response     │
   │                                 │
   ─── FETCH_LOG (req: per-group ──▶ Leader
       follower_ewm cursor)          │ (6) Update followerEwmByGroup
                                     │ (7) maybeAdvanceCEW → min(EWM across ISR)

Client ── FETCH_LOG (projection ──▶ Leader
              touches enrichment)    │ (8) Cap = min(HW, min(CEW_g across projected groups))
                                     │ (9) EnrichmentMerger up to cap
```

### 3.3 Wire format

Two additions to `fluss-rpc/src/main/proto/FlussApi.proto`:

`PbFetchLogReqForBucket`:
- `repeated PbFollowerEwmRequest follower_ewm_requests` — one per registered
  group, carrying the follower's current EWM cursor.

`PbFetchLogRespForBucket`:
- `repeated PbEnrichmentBatchForGroup enrichment_batches` — Arrow records +
  parallel `int64[] source_offsets` per group.
- `repeated PbCommittedEnrichmentWatermark committed_ewms` — leader's view of
  CEW per group on this bucket.

Empty-list defaults (proto3) preserve backwards compatibility: tables without
column groups, and pre-Phase-E peers, see no change.

## 4. Decisions

### 4.1 Replication transport: piggyback on `FETCH_LOG`

| Option | Mechanism | Verdict |
|---|---|---|
| **A. Piggyback on `FETCH_LOG`** | Leader's fetch response carries enrichment alongside base records; follower advertises per-group cursor in request. | **Chosen.** |
| B. Separate `FETCH_ENRICHMENT` RPC | Parallel pull stream per (bucket, group). | Rejected — duplicates the puller, ack state machine, gap recovery, retry/backoff logic in `ReplicaFetcherThread`. Two sources of "is this committed?" truth. |
| C. Push-on-write from leader | Leader forwards `PRODUCE_LOG_COLUMNS` to followers within its own handler. | Rejected — inverts who-talks-to-whom (today only followers initiate); requires leader-side connections to N-1 followers; producer is then blocked by the slowest follower. |

**Why A despite the wire complexity.** The same per-follower fetch state machine
already drives base-log HW. Reusing it means one ack source of truth and zero
new failure modes. The cost is a slightly fatter `FETCH_LOG` request/response;
the benefit is no new connection management, no new gap-recovery logic, and no
new "is this committed?" decision point.

### 4.2 CEW = `min(EWM across ISR)`

Mirrors base-log HW semantics. Out-of-sync replicas don't hold up commit
progress; once they rejoin ISR they catch up.

**Invariant:** `CEW_g ≤ leader_HW + 1` (you cannot commit enrichment at a
source offset whose base record was never produced).

**Not an invariant:** `CEW_g ≤ HW`. Enrichment commit and base commit are
independent — CEW will typically *lag* HW by a few fetches' worth.

### 4.3 New-leader CEW: derive from local EWM, no checkpoint

On `Replica.onBecomeNewLeader`, set `CEW_g := EWM_g_local` for every registered
group. No persisted checkpoint, no extra fsync.

**Correctness under clean election.** Previous `CEW_g` was `min(EWM across ISR)`,
which by definition was ≤ this follower's `EWM_g_local`. New CEW therefore does
not regress.

**Failure mode under unclean election.** An out-of-sync follower elected leader
has `EWM_g_local < previous CEW_g`. CEW regresses; clients re-issuing the same
fetch see "phantom" data go missing. **Accepted** as the same shape as base-log
unclean-election data loss; the durable fix is the leader-epoch cache
(`ReplicaFetcherThread.java:603-604`) that will fence stale leaders for both
streams in one shot.

Considered alternatives:
- **Persist CEW to disk per replica.** Adds an fsync to the hot path; recovery
  must reconcile checkpoint vs disk. Rejected for complexity vs. the rare case.
- **Persist CEW to ZooKeeper alongside `LeaderAndIsr`.** Write rate too high;
  ZK is for low-frequency state.

### 4.4 Truncation: strict, base-first, with recovery clamp

`LogTablet.truncateTo` and `truncateFullyAndStartAt` (`LogTablet.java:1120-1185`)
currently leave enrichment segments untouched. After replication lands this
becomes a corruption hazard: a replica truncating base log to offset N must
also drop enrichment with `source_offset >= N`, otherwise on next replication
the leader will append enrichment that contradicts the replica's stale data,
violating the contiguous-from-EWM contract enforced in
`Replica.appendColumnsAsLeader`.

Three decisions resolved together:

1. **Trigger: mirror only.** Enrichment truncation fires *exclusively* as a
   side effect of base-log truncation. No enrichment-only truncation path.
   Enrichment exists to annotate base records; if a base record disappears,
   the corresponding enrichment is meaningless and unsafe to keep.

2. **Strict, not lazy.** On every base truncate to offset N, walk every
   registered group, call `EnrichmentSegment.truncateTo(N)` (drop
   `OffsetIndex` entries with `offset >= N`, truncate the `FileLogRecords`
   to the file position of the first dropped entry), reset
   `EWM_g := min(EWM_g, N)`. Lazy gating (keep stale entries, gate reads at
   `min(EWM_g, localLogEndOffset)`) was rejected because the
   contiguous-from-EWM write contract requires the file's tail to track EWM —
   splitting them forces a rewrite of the append path, and disk savings are a
   Tier-3 concern anyway.

3. **Order: base first, then enrichment, with recovery clamp.** A multi-file
   truncate cannot be made atomic. Truncating base before enrichment leaves a
   crash window in which `EWM_g > localLogEndOffset`; the recovery clamp in
   E.5d (`discoverEnrichmentSegments` post-process) closes that window by
   re-truncating dangling enrichment at startup. The reverse order (enrichment
   first) creates a window where base is ahead of enrichment, which has no safe
   recovery — we would be inferring base-log truncation from enrichment state.

**Phase ordering:** E.5b (truncation fix) lands **before** E.3 (replication
wire-up). Without that ordering the latent bug becomes a live data-corruption
scenario the first time an unclean truncation hits a replica.

Adds `EnrichmentSegment.truncateTo(long sourceOffsetExclusive)` and wires it
into both `LogTablet.truncateTo` and `LogTablet.truncateFullyAndStartAt`.

### 4.5 ISR coupling

EWM lag is **not** considered when computing ISR membership. Justification:
enrichment is always downstream of base log (you can only enrich offset N
after base offset N is replicated). A follower keeping up with base will keep
up with enrichment within a fetch round-trip. Revisit if production
observation shows otherwise.

## 5. Phasing

```
E.1  Design ADR (this doc)              ─┐
E.5b Truncation fix                     ─┤  ★ Lands first — see §4.4
E.2  CEW state types + accessors        ─┤
E.3a Wire format (proto)                ─┤
E.3b Leader-side enrichment read         │
E.3c Follower-side persist + cursor      │
E.3d Leader-side CEW advance             │
E.4  Read-side switch to CEW            ─┤  Depends on E.3* — otherwise CEW=-1 always
E.5a New-leader CEW init                ─┤
E.5c Catch-up after follower gap         │
E.5d Same-node recovery clamp            │
E.6  ISR coupling — deferred (Option A)  │
E.7  Metrics + observability            ─┘
```

**Critical path:** E.5b → E.2 → E.3a → E.3{b,c,d} → E.4. E.5{a,c,d} can land
parallel to E.4. E.6 deferred. E.7 anytime after E.3.

## 6. Open questions

### 6.1 Backpressure / fairness across groups

If group A has a 100MB enrichment backlog and group B is current, how does the
leader split bandwidth in a single fetch response?

**Proposal:** new config `replication.fetch.enrichment.max-bytes` (default
8MB), applied per-group, round-robin across registered groups within a single
bucket response. Pin down before E.3a — affects whether the proto needs a
per-group budget hint.

### 6.2 Schema evolution across replication

If a column group's schema changes between when an enrichment batch is written
on the leader and replicated to a follower, the follower's deserialization may
fail. `LogRecordReadContext` already plumbs `SchemaGetter`, but Phase D has not
exercised cross-schemaId paths. Defer test coverage to E.7; ensure error path
is loud, not silent.

### 6.3 Per-group fetch session bookkeeping

`FetchSession` caches per-bucket fetch metadata to skip redundant request
fields. Adding per-group EWM cursor expands this cache by O(groups) per
bucket. Confirm session size growth is acceptable before E.3a.

## 7. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Unclean election regresses CEW | Clients observe "phantom" data go missing on retry | Accepted; documented; resolved alongside leader-epoch cache (out of scope here) |
| E.3 lands before E.5b | Stale enrichment beyond truncation → data corruption on next write | Strict phase ordering; E.3 PR cannot merge without E.5b prereq |
| Replication backlog on enrichment-heavy workload | CEW lags; projecting clients see stale-bounded reads | Backpressure config (see §6.1); operational metrics in E.7 |
| Schema evolution mid-replication | Follower deserialize fails; replication stalls | E.7 integration test; verify error path is loud |
| Wire/CPU regression for non-enrichment tables | The 99% case slows down for the 1% feature | Empty-list proto defaults; benchmark before/after on a non-enrichment table |

## 8. Test strategy

| Phase | Test |
|---|---|
| E.5b | `EnrichmentSegment.truncateTo` unit. ITCase: unclean-election truncation, verify enrichment truncates correspondingly. |
| E.2 | `LogTablet` unit: CEW monotonic, defaults to -1L. `Replica` unit: `maybeAdvanceCEW` correct under ISR shrink/expand. |
| E.3a | Proto round-trip in `fluss-rpc`. Compatibility: pre-Phase-E request to post-Phase-E leader → no enrichment in response. |
| E.3b | `LogTabletTest` adds `readEnrichmentForFollower`; verify range correctness for various `(fromEwm, budget)`. |
| E.3c | 2-server `FlussClusterExtension`: write base+enrichment on leader, verify follower's `*.col.<g>.log/.index` files exist with matching content. |
| E.3d | `Replica` unit: simulate two follower fetches with different EWM, assert CEW = min. |
| E.4 | `ColumnGroupEWMITCase` extended to 3-server: kill a follower, verify CEW does not advance until rejoin; restart, verify catch-up. |
| E.5a | Failover ITCase: write with `acks=-1` until CEW=N, kill leader, verify projecting client fetches see exactly `[0, N)`. |
| E.5c+d | Crash mid-write, restart, verify recovery clamps EWM to base LEO. |
| E.7 | Metrics presence & monotonicity (`enrichment.committed_ewm` ≤ `enrichment.local_ewm`). |

---

This document is the **Phase E.1 ADR**. Subsequent commits should reference it
by path. Open questions in §6 must be resolved (or explicitly punted) before
their dependent phase starts.
