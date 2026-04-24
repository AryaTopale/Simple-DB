# SimpleRA Phase 2 — Implementation Report

> **Course:** Data Systems – Spring 2026  
> **Commands Implemented:** `SETBUFFER` · `SORT` · `JOIN` · `GROUP BY`

---

## Table of Contents

1. [SETBUFFER](#1-setbuffer)
2. [SORT](#2-sort)
3. [HASH JOIN](#3-hash-join)
4. [GROUP BY](#4-group-by)
5. [Assumptions](#5-assumptions)


---

## 1. SETBUFFER

### Syntax

```
SETBUFFER K        (2 ≤ K ≤ 10)
```

### Overview

`SETBUFFER` controls the session-wide memory budget: it sets `MAX_BLOCKS_IN_MEMORY` to `K`, which every subsequent command uses to limit the number of disk blocks resident in main memory at any time.

### Implementation Flow

```
┌─────────────────────────────────────────────────────────────┐
│                      SETBUFFER K                            │
└────────────────────────────┬────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  Validate K     │
                    │  (2 ≤ K ≤ 10)  │
                    └────────┬────────┘
                    FAIL ◄───┤───► PASS
                    (error)  │
                             │
              ┌──────────────▼──────────────┐
              │  MAX_BLOCKS_IN_MEMORY = K   │
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │  bufferManager.trimPool()   │
              │  (evict excess pages)       │
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │  "Buffer size set to K"     │
              └─────────────────────────────┘
```

### Logic

| Step | Action |
|------|--------|
| **Validate** | Semantic parser checks `2 ≤ K ≤ 10`; prints error and returns on failure |
| **Apply** | `MAX_BLOCKS_IN_MEMORY = (uint) parsedQuery.setBufferCount` |
| **Evict** | `bufferManager.trimPool()` evicts any buffer-pool pages beyond the new limit |
| **Confirm** | Prints confirmation message |

### Block Accesses

`SETBUFFER` performs **zero disk reads**. `trimPool()` may write dirty evicted pages to disk, but no new reads are issued.

### Error Handling

- `K < 2` or `K > 10` → prints error, does **not** modify `MAX_BLOCKS_IN_MEMORY`
- Can be called multiple times; each call replaces the previous value
- Default if never called: **10 blocks**

---

## 2. SORT

### Syntax

```
SORT <table-name> BY <col1>, <col2>, ... IN <ASC|DESC>, <ASC|DESC>, ...
     [TOP X] [BOTTOM Y]
```

### Algorithm: K-way Multi-Phase External Merge Sort

Follows **Elmasri & Navathe §18.2**. Sorting is **in-place**: only the table's temp pages on disk are modified; the original CSV in `/data` is unchanged until the user explicitly exports.

The key idea: with `K = MAX_BLOCKS_IN_MEMORY` blocks of memory, Phase 0 creates initial sorted runs of size `K` pages; subsequent merge phases reduce the run count by merging `K−1` runs at a time using a min-heap until a single run remains.

---

### Phase 0 — Initial Run Generation

```
  Disk (table pages)                    Temp storage
  ────────────────────                  ─────────────────────────
  [ Page 0  ]  ─┐
  [ Page 1  ]   ├─► Load K pages ─► Sort in memory ─► Write run
  [   ...   ]   │   (bufferManager        (stable         files
  [ Page K-1]  ─┘    .getPage())        merge sort)
                                         │
  [ Page K  ]  ─┐                        │  SORT_T_R0_Page0
  [ Page K+1]   ├─► Load K pages ─► ... │  SORT_T_R0_Page1
  [   ...   ]  ─┘                        │  SORT_T_R1_Page0
                                          │  ...
  [continues]
```

- Read **K pages** at a time using `bufferManager.getPage()`
- Each row is wrapped in `RowWithIndex { row, originalIndex }` for stability tracking
- In-memory custom **stable merge-sort** (no `std::sort`) produces a sorted chunk
- Written to temp files `SORT_<table>_R<runIdx>_Page<pageIdx>` via `bufferManager.writeTempPage()`
- Memory used: exactly **K blocks** (one per page read)

---

### Merge Phases — (K−1)-way Merge

```
  Phase 1:
  Run 0 ──┐
  Run 1 ──┤
  Run 2 ──┤──► (K-1)-way ──► Merged Run 0'
  ...     │    min-heap
  Run K-2 ─┘

  Run K-1 ──┐
  Run K   ──┤──► (K-1)-way ──► Merged Run 1'
  ...       │    min-heap
  Run 2K-3──┘

  ... repeat until 1 run remains ...

  Phase 2:
  Run 0'──┐
  Run 1'──┤──► (K-1)-way ──► Single sorted run
  ...     │    min-heap
```

**Memory layout per merge group:**

```
  ┌──────────────────────────────────────────────────────────────┐
  │  Memory (K blocks)                                           │
  │  ┌──────────┐ ┌──────────┐     ┌──────────┐ ┌──────────┐   │
  │  │ Run 0    │ │ Run 1    │ ... │ Run K-2  │ │ Output   │   │
  │  │ Page buf │ │ Page buf │     │ Page buf │ │ Buffer   │   │
  │  └──────────┘ └──────────┘     └──────────┘ └──────────┘   │
  │   K−1 input buffers (1 page each)            1 output page  │
  └──────────────────────────────────────────────────────────────┘
```

- A `std::priority_queue` (min-heap) selects the globally smallest row each step
- **Stability** in merge: when two rows are equal on all sort keys, the **lower run index** wins (earlier run = earlier in original order)
- New runs written as `SORT_T_NR<i>_Page<j>`, then renamed to `SORT_T_R<i>_Page<j>` after old runs are deleted
- `bufferManager.clearPool()` is called between phases to ensure stale pages are evicted

---

### Final Write-back

```
  Sorted Run (temp)          Table pages (disk)
  ─────────────────          ──────────────────
  R0_Page0 ──► read ──┐
  R0_Page1 ──► read   ├──► bufferManager.writePage(table, p, ...)
  ...                 │     (only pages in [firstPage, lastPage] range)
  R0_PageN ──► read ──┘
```

- Reads the single sorted run sequentially
- Writes back into the **original table pages** using `bufferManager.writePage()`
- Only rows within `[startRow, endRow)` are overwritten; rows outside the range remain untouched
- Temp sort files are deleted after write-back

---

### Stability Guarantee

```cpp
struct RowWithIndex {
    vector<int> row;
    long long originalIndex;   // global row index for tie-breaking
};
```

Phase 0 comparator:
```
  if rows differ on any sort key → use sort key order
  if all sort keys equal         → use originalIndex (lower = earlier = wins)
```

Merge phase comparator (heap):
```
  if rows differ on any sort key → use sort key order
  if all sort keys equal         → use run index (lower run = earlier original order)
```

---

### TOP / BOTTOM Support

```
  Total rows: N
  ─────────────────────────────────────────────────────
  │◄──── TOP X ────►│◄──── middle ────►│◄── BOTTOM Y ─►│
  └────────┬─────────┘                  └───────┬────────┘
           │                                    │
    externalMergeSort                    externalMergeSort
    (startRow=0, n=X)                    (startRow=N-Y, n=Y)
           │                                    │
    [sorted independently]              [sorted independently]
```

- **`TOP X`**: sorts rows `[0, X)` — `externalMergeSort(table, cols, strats, 0, X)`
- **`BOTTOM Y`**: sorts rows `[N-Y, N)` — `externalMergeSort(table, cols, strats, N-Y, Y)`
- **Both present**: two sequential independent sort calls; buffer pool cleared between them
- **Middle rows**: not touched — retained in original order
- Each sort gets its own temp file prefix to avoid collisions

---

### Block Accesses Analysis

Let **N** = number of pages in the sort range, **K** = `MAX_BLOCKS_IN_MEMORY`.

```
  Phase 0:   N reads  +  N writes  =  2N
  Each merge pass:    N reads  +  N writes  =  2N
  Number of merge passes:  ⌈log_{K-1}(N/K)⌉
  Write-back: N reads (sorted run) + N writes (table) = 2N

  Total ≈ 2N · (1 + ⌈log_{K-1}(N/K)⌉ + 1)
        = O(N · log_{K-1}(N))
```

| Phase | Reads | Writes |
|-------|-------|--------|
| Phase 0 (runs) | N | N |
| Each merge pass | N | N |
| Write-back | N | N |
| **Total** | **N(2 + passes)** | **N(2 + passes)** |

---

### Error Handling

| Condition | Error Message |
|-----------|---------------|
| Table does not exist | `SEMANTIC ERROR: Relation doesn't exist` |
| Column not in table | `SEMANTIC ERROR: Column doesn't exist in relation` |

---

## 3. HASH JOIN

### Syntax

```
Result <- JOIN <table1>, <table2> ON <join-condition>
          [WHERE <table>.<col> <op> <number>]
          [PROJECT <table1>.<col>, <table2>.<col>, ...]
```

**Join condition types:**
- **Attribute equality:** `A.col == B.col`
- **Arithmetic expression:** `A.col + B.col == N`  or  `A.col - B.col == N`

### Algorithm: Partition-Hash Join

Follows **Elmasri & Navathe §18.4.4**. Uses hashing as the primary mechanism; nested-loop and sort-merge joins are not used.

---

### Overview Diagram

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                     executeJOIN()                               │
  │                                                                 │
  │  Table R ──► partitionTable(R, colR, K−1 buckets) ──► R_0..K-2 │
  │  Table S ──► partitionTable(S, colS, K−1 buckets) ──► S_0..K-2 │
  │                                                                 │
  │  for i in 0..K-2:                                              │
  │    probeOrRecurse(R_i, S_i)                                     │
  │         │                                                        │
  │         ├─► R_i fits in B-2 blocks?                             │
  │         │      YES ──► Build hash table from R_i                │
  │         │              Probe with each row of S_i               │
  │         │              Apply WHERE + PROJECT → emit row         │
  │         │                                                        │
  │         └─► NO  ──► Re-partition R_i, S_i with next prime      │
  │                      Recurse on sub-buckets                     │
  │                                                                 │
  │  resultTable.blockify() ──► tableCatalogue.insertTable()        │
  └─────────────────────────────────────────────────────────────────┘
```

---

### Phase 1 — Partitioning

```
  Table R (|R| rows)                Table S (|S| rows)
  ──────────────────                ──────────────────
  Cursor scans R                    Cursor scans S
  row by row                        row by row
        │                                 │
        ▼                                 ▼
  hash(R.joinCol, prime_0)         hash(S.joinCol, prime_0)
  mod (K−1) partitions             mod (K−1) partitions
  [for arith: hash S on N−s]
        │                                 │
  ┌─────▼──────────────────┐       ┌─────▼──────────────────┐
  │ R_part_0, R_part_1,... │       │ S_part_0, S_part_1,... │
  └────────────────────────┘       └────────────────────────┘
```

- Number of partitions: `B − 1` (B = `MAX_BLOCKS_IN_MEMORY`)
- Hash function: `((key % prime) + prime) % prime % numParts`
- Prime sequence: `{7, 11, 13, 17, 19, 23, 29, 31, 37, 41}` — successive primes used at each recursive depth to avoid same-bucket clustering
- For arithmetic join `A.a ± B.b == N`: S is hashed on a key-transformed value so matching rows always land in the **same bucket**:

```
  A + B == N  ⟹  hash B on (N − B.col)   so hash(A.col) == hash(N − B.col)
  A − B == N  ⟹  same transform (N − B.col)
```

---

### Phase 2 — Build & Probe

```
  For each bucket pair (R_i, S_i):

  CASE 1: R_i fits in (B−2) memory blocks
  ─────────────────────────────────────────
  ┌────────────────────────────────────────────────────────────────┐
  │  Memory (B blocks)                                             │
  │  ┌──────────────────────────────────┐  ┌────────┐  ┌───────┐ │
  │  │  Hash table built from R_i       │  │ 1 page │  │Output │ │
  │  │  unordered_map<int,              │  │ of S_i │  │buffer │ │
  │  │    vector<row>>                  │  │        │  │       │ │
  │  └──────────────────────────────────┘  └────────┘  └───────┘ │
  │         B−2 blocks                       1 block    1 block   │
  └────────────────────────────────────────────────────────────────┘

  Probe: for each S_i row, look up in hash table:
    - ATTR_EQUAL:  key = S.col → O(1) lookup
    - ARITH_EXPR:  scan all R_i rows, check A.col ± B.col == N

  CASE 2: R_i does NOT fit in memory
  ────────────────────────────────────
  Re-partition R_i and S_i using next prime → probeOrRecurse(depth+1)
```

---

### WHERE & PROJECT — Inline Filtering

```
  Candidate combined row [R_cols | S_cols]
          │
          ├─► WHERE predicate?
          │     YES → evaluate A.col op number
          │           FAIL → discard row
          │           PASS ──►
          │
          └─► PROJECT clause?
                YES → select only specified columns in specified order
                NO  → output all R cols then all S cols
                │
                ▼
          resultTable.writeRow(outRow)
```

`emitRow()` applies both filters in a single pass — **zero extra I/O**.

---

### Recursive Re-partitioning

```
  Depth 0:  prime=7   → buckets 0..K-2
  Depth 1:  prime=11  → sub-buckets
  Depth 2:  prime=13  → sub-sub-buckets
  ...
  Depth 9:  prime=41  (last prime in sequence)

  Each depth uses a different prime, breaking same-key clusters
  that would otherwise re-hash to the same bucket indefinitely.
```

Sub-tables are created in the catalogue, probed recursively, then **deleted** immediately after to free disk space.

---

### Block Accesses Analysis

Let **|R|**, **|S|** = number of pages in R and S respectively, **B** = buffer size.

```
  Partition Phase:
    Read R:   |R| pages
    Write R:  |R| pages   (K−1 partition files)
    Read S:   |S| pages
    Write S:  |S| pages   (K−1 partition files)

  Build & Probe (no recursion):
    Read R:   |R| pages   (build hash table, each bucket read once)
    Read S:   |S| pages   (probe, each bucket read once)

  Total (ideal, no recursion) = 3(|R| + |S|)
```

| Phase | Reads | Writes |
|-------|-------|--------|
| Partition R | \|R\| | \|R\| |
| Partition S | \|S\| | \|S\| |
| Build (R) | \|R\| | 0 |
| Probe (S) | \|S\| | result |
| **Total** | **3(\|R\|+\|S\|)** | **2(\|R\|+\|S\|) + result** |

> Each level of recursion adds another `2(|bucket_R| + |bucket_S|)` accesses, but bucket sizes shrink with each level.

---

### Error Handling

| Condition | Error Message |
|-----------|---------------|
| Either table does not exist | `SEMANTIC ERROR: Relation doesn't exist` |
| Join column not in table | `SEMANTIC ERROR: Column doesn't exist in relation` |
| WHERE column not in table | `SEMANTIC ERROR: Column doesn't exist in relation` |
| PROJECT column not in table | `SEMANTIC ERROR: Column doesn't exist in relation` |
| No rows satisfy join + WHERE | Result table not created; message printed |

---

## 4. GROUP BY

### Syntax

```
Result1, Result2, ... <- GROUP BY <attr1>, <attr2>, ... FROM <table>
                         HAVING <agg-expr> <op> <agg-expr|number>
                         RETURN <agg1>, <agg2>, ...
```

Each grouping attribute is processed **independently**, producing its own two-column result table: `[groupAttr, returnAggregate]`.

### Algorithm: Two-Phase Hash Aggregation with External Sort

```
  For each grouping attribute i:

  ┌──────────────────────────────────────────────────────────────────┐
  │                      executeGROUP_BY (gi)                        │
  │                                                                  │
  │  Source table ──► Cursor (row by row)                            │
  │                        │                                         │
  │              Project: [groupKey, accum_cols...]                  │
  │                        │                                         │
  │              ┌─────────▼─────────┐                              │
  │              │  processStream()  │ (Phase 1: hash partition)    │
  │              └─────────┬─────────┘                              │
  │                        │                                         │
  │          ┌─────────────┼─────────────┐                         │
  │          ▼             ▼             ▼                          │
  │      Partition 0  Partition 1  ...  Partition P-1              │
  │      (temp pages) (temp pages)      (temp pages)               │
  │          │                                                       │
  │          ▼  (Phase 2: per-partition sort-aggregate)             │
  │   externalSort → stream-scan → accumulate → HAVING → emit       │
  │          │                                                       │
  │          ▼                                                       │
  │   resultTable [groupAttr | returnAggregate]                     │
  └──────────────────────────────────────────────────────────────────┘
```

---

### Phase 1 — Hash Partitioning

```
  Source table (N rows)
        │
        │  Cursor.getNext() — 1 page in memory at a time
        ▼
  Project each row → [groupKey, col1_val, col2_val, ...]
        │
        │  hash(groupKey, depth_salt) % numPartitions
        ▼
  ┌─────────────────────────────────────────────────────────┐
  │  In-memory partition buffers (1 page each)              │
  │  partBufs[0]  partBufs[1]  ...  partBufs[P-1]          │
  │  ─────────    ─────────         ─────────               │
  │  When full → flush to temp page via writeTempPage()     │
  └─────────────────────────────────────────────────────────┘

  Memory at any point:
    1 page (Cursor)  +  P pages (partition buffers)  ≤  B blocks
    ⟹  P ≤ B − 1
```

**Hash salt per recursion depth** (Knuth multiplicative hash):
```
p = (abs(key) + depth × 2654435761) % numPartitions
```
This distributes rows with the same key across different partitions when recursing, avoiding infinite same-bucket loops.

---

### Phase 2 — Per-partition Sort + Stream Aggregate

```
  For each partition p:

  ┌────────────────────────────────────────────────────────┐
  │  Partition p (fits in B−2 pages)                       │
  │                                                        │
  │  Read pages back via getTempPage()                     │
  │         │                                              │
  │  External sort by groupKey                             │
  │  (K-way merge sort on projected schema)                │
  │         │                                              │
  │  Sorted stream:                                        │
  │  key=1,1,1,2,2,3,3,3,...                               │
  │         │                                              │
  │  Accumulate per key:                                   │
  │  ┌──────────────────────────────────┐                 │
  │  │ ColAccum {sum, count, min, max}  │  (for each col) │
  │  └──────────────────────────────────┘                 │
  │  On key change → evaluate HAVING → emit result row    │
  └────────────────────────────────────────────────────────┘
```

**Accumulator struct** tracks all five aggregate functions simultaneously:
```
  struct ColAccum {
    long long sum   = 0;    // for SUM, AVG
    int       count = 0;    // for COUNT, AVG
    int       minVal = +∞;  // for MIN
    int       maxVal = -∞;  // for MAX
  };
```

This means a single pass computes **all** needed aggregates — HAVING and RETURN are both evaluated from the same accumulators.

---

### HAVING Evaluation

```
  HAVING LHS op RHS

  LHS = computeAggregate(havingLHS.func, accums[havLhsIdx])
  RHS = (havingRHSIsAggregate)
          ? computeAggregate(havingRHS.func, accums[havRhsIdx])
          : havingRHSValue  (constant number)

  Supported: MAX, MIN, COUNT, SUM, AVG
  Operators:  ==  !=  >  >=  <  <=
```

Groups **not** satisfying HAVING are discarded. If **no** group passes, the result table is not inserted into the catalogue.

---

### Recursive Repartitioning

```
  If partition p is too large (totalRows > (B−2) × rowsPerPage):

  Recursively call processStream() on partition p
  with depth+1 and a different hash salt

  Max recursion depth: 8 (hard-coded guard)
  Beyond depth 8: fall back to single-partition sort-aggregate
  (guarantees termination even for pathological data)
```

---

### Multiple Grouping Attributes

```
  Query: R1, R2, R3 <- GROUP BY A, B, C FROM T HAVING ... RETURN agg1, agg2, agg3

  Processed independently:

  Group by A → RETURN agg1 → R1: [A | agg1(col)]
  Group by B → RETURN agg2 → R2: [B | agg2(col)]
  Group by C → RETURN agg3 → R3: [C | agg3(col)]

  Each uses the same HAVING condition but evaluates it per-group
  on the respective grouping attribute.
```

---

### Result Column Naming

| RETURN Expression | Result Column Name |
|-------------------|--------------------|
| `MAX(Salary)`     | `MAXSalary`        |
| `MIN(Years)`      | `MINYears`         |
| `SUM(Expenses)`   | `SUMExpenses`      |
| `AVG(Salary)`     | `AVGSalary`        |
| `COUNT(col)`      | `COUNTcol`         |
| `COUNT(*)`        | `COUNT`            |

---

### Block Accesses Analysis

Let **N** = source table pages, **P** = number of partitions, **B** = buffer size.

```
  Phase 1 (partition):
    Read source:     N pages
    Write partitions: N pages total across P partition files

  Phase 2 per partition (sort-aggregate):
    Read partition:  N/P pages
    Sort reads+writes: 2·(N/P)·⌈log_{K-1}(N/P)⌉
    Scan for agg:    N/P pages
    Write result:    tiny (result rows)

  Phase 2 total (all partitions): ≈ 2N + sort overhead

  Grand total ≈ 3N  (ideal, no recursion, small sort overhead)
```

| Phase | Memory Usage | I/O |
|-------|-------------|-----|
| Phase 1 | 1 cursor + P buffers ≤ B | 2N |
| Phase 2 | 1 read page + groupMap ≤ B−2 | ≈ N + sort |
| **Total** | **≤ B blocks at all times** | **≈ 3N per attribute** |

---

### Error Handling

| Condition | Error Message |
|-----------|---------------|
| Table does not exist | `SEMANTIC ERROR: Relation doesn't exist` |
| Grouping attribute not in table | `SEMANTIC ERROR: Group By column doesn't exist in relation` |
| HAVING column not in table | `SEMANTIC ERROR: Having column doesn't exist in relation` |
| RETURN column not in table | `SEMANTIC ERROR: Return column doesn't exist in relation` |
| No group satisfies HAVING | Result table not created (empty result) |

---

## 5. Assumptions

- **SORT always re-reads from the original CSV** before sorting (re-blockifies from disk), so repeated `SORT` calls on the same table produce a deterministic result from the original data — intermediate sorted state from a previous call does not persist.

- **TOP X and BOTTOM Y ranges never overlap** (guaranteed by the specification). No overlap-check is performed at runtime.

- **SORT temp file prefix** is `SORT_<tableName>`. If two concurrent sort operations on the same table were issued (which the sequential command flow prevents), temp file names could collide. We assume the command flow is sequential.

- **JOIN arithmetic conditions** assume integer values and the result `A.col ± B.col` fit within a 32-bit signed integer. No overflow detection is performed.

- **JOIN result table uniqueness** is enforced by the semantic parser (result name must not already be in the catalogue). We do not add a runtime check in `executeJOIN`.

- **GROUP BY result table names** are all distinct and not already in the catalogue, as guaranteed by the specification. Duplicate result names would cause a catalogue conflict.

- **Recursive re-partitioning** in both JOIN and GROUP BY is bounded: JOIN by the prime list length (10 levels), GROUP BY by the hard-coded depth guard of 8. Pathological data (e.g., all rows with the same join key) may trigger recursion; at maximum depth, GROUP BY falls back to a single-partition sort-aggregate.

- **`bufferManager.trimPool()`** correctly evicts pages beyond the pool limit and is already implemented in the base codebase from Phase 1.

- **BLOCK_SIZE and BLOCK_COUNT** remain at their default values and are not modified by any Phase 2 command.

- **Private class members** are never accessed directly; all interaction with `Table`, `Page`, `Cursor`, and `BufferManager` goes through their public API.

- **`COUNT(*)` in GROUP BY** counts all rows in the group regardless of column values. The special column name `"*"` is mapped to a dedicated `starAccum` accumulator slot (index `starIdx`) that is incremented for every row.

---

