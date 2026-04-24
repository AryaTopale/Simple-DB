# SimpleRA Phase 3 — Concurrency Control Report

> **Course:** Data Systems – Spring 2026
> **Technique Implemented:** Strict Two-Phase Locking (S2PL) with Wait-Die Deadlock Prevention

---

## Table of Contents

1. [Overview](#1-overview)
2. [Why Strict 2PL over Timestamp Ordering](#2-why-strict-2pl-over-timestamp-ordering)
3. [Command Syntax and Integration](#3-command-syntax-and-integration)
4. [Data Structures](#4-data-structures)
5. [Algorithm — Strict 2PL with Wait-Die](#5-algorithm--strict-2pl-with-wait-die)
6. [Detailed Walkthrough (Sample Test Case 1)](#6-detailed-walkthrough-sample-test-case-1)
7. [Detailed Walkthrough (Sample Test Case 2)](#7-detailed-walkthrough-sample-test-case-2)
8. [Restart Mechanism](#8-restart-mechanism)
9. [Integration with SimpleRA](#9-integration-with-simplera)
10. [Error Handling](#10-error-handling)
11. [Assumptions](#assumptions)
12. [Team Contributions](#team-contributions)

---

## 1. Overview

Phase 3 implements a **simulation** of page-level concurrency control on top of the existing SimpleRA engine. The system processes a schedule of transactions from an input file and produces an execution log in an output file, showing every lock request, grant, wait, abort, rollback, and restart decision taken by the protocol.

The implementation is entirely a **simulation** — actual page data is not modified; only lock metadata and the execution log are produced. Actual page reads via `bufferManager.getPage()` are issued to conform to the constraint that the cursor/bufferManager must be used, but their results are discarded.

### Command

```
TRANSACTION <filename>
```

- `<filename>` is a schedule file located in `../data/`
- Output is written to `../data/<filename_without_ext>_output.txt`

---

## 2. Why Strict 2PL over Timestamp Ordering

We chose **Strict Two-Phase Locking (S2PL)** over Strict Timestamp Ordering (STO) for the following reasons:

### 2.1 Conceptual clarity for simulation

S2PL operates on a lock table with explicit grant/wait/die decisions at each operation step. The output log therefore maps 1-to-1 with the schedule — every READ or WRITE either immediately prints `Lock granted` or triggers a wait/die decision. This makes it easy to trace and verify correctness.

STO, by contrast, involves comparing timestamps against per-page read/write timestamps and can require a "wait for uncommitted write" state (as seen in the Sample Test Case 2 timestamp output: `WAIT because A 20 was written by uncommitted transaction T3`). Handling this deferred-write wait correctly in a sequential simulation adds significant complexity around re-queuing committed-page notifications.

### 2.2 No dirty-read problem in S2PL

Under Strict 2PL, exclusive locks are held until commit. This automatically prevents any transaction from reading uncommitted data (dirty reads), recoverability is guaranteed, and cascading rollbacks cannot occur. STO requires an additional "write buffer" or "Thomas write rule" implementation to achieve the same recoverability guarantees.

### 2.3 Simpler restart model

Under S2PL with wait-die, a restarted transaction simply re-executes its original READ/WRITE/COMMIT operations in sequence. Since the restarted transaction receives a new high timestamp, it is always older than or equal to any currently-active transaction and thus will not die again — it can always wait if needed and will eventually acquire its locks. Under STO, a restarted transaction receives a new timestamp and must re-validate all operations against current page timestamps, which requires more bookkeeping.

### 2.4 Comparison Table

| Property | Strict 2PL + Wait-Die | Strict Timestamp Ordering |
|---|---|---|
| Deadlock prevention | Wait-die protocol | Timestamp order (no deadlock possible) |
| Dirty reads | Prevented (exclusive held to commit) | Prevented (write buffer / strict mode) |
| Cascading rollbacks | None | None (strict mode) |
| Restart complexity | Simple replay with new timestamp | Re-validate all ops against page TSs |
| Output predictability | Fully deterministic from schedule | Can require deferred WAIT on uncommitted writes |
| Implementation complexity | Moderate | Higher (page TS + write buffer) |

---

## 3. Command Syntax and Integration

### 3.1 Syntax

```
TRANSACTION <input_file>
```

**Example:**
```
TRANSACTION schedule1.txt
```

The input file must be in `../data/`. The output file is created at `../data/schedule1_output.txt`.

### 3.2 Integration with the SimpleRA Pipeline

The command follows the existing three-stage pipeline:

```
Tokenization (server.cpp)
        │
        ▼
syntacticParseTRANSACTION()       [syntacticParser.cpp]
  • Validates exactly 2 tokens: "TRANSACTION" + filename
  • Sets parsedQuery.queryType = TRANSACTION
  • Sets parsedQuery.transactionFileName = filename
        │
        ▼
semanticParseTRANSACTION()        [semanticParser.cpp]
  • Checks ../data/<filename> exists via stat()
  • Returns error if file not found
        │
        ▼
executeTRANSACTION()              [executors/transaction.cpp]
  • Parses schedule file line by line
  • Runs S2PL simulation
  • Writes execution log to output file
```

### 3.3 Schedule File Format

Each line contains one of:

| Line format | Meaning |
|---|---|
| `BEGIN T<n>` | Start transaction n |
| `READ T<n> <table> <page>` | Transaction n reads the given page |
| `WRITE T<n> <table> <page>` | Transaction n writes the given page |
| `COMMIT T<n>` | Transaction n commits |

The first `BEGIN` assigns timestamp 1; each subsequent operation increments the logical clock by 1.

---

## 4. Data Structures

### 4.1 Lock Table

```cpp
map<string, PageLock>  lockTable;
```

The lock table maps a **page key** (e.g., `"student 5"`) to a `PageLock` struct:

```cpp
struct PageLock {
    LockType  type            = LOCK_NONE;
    set<int>  sharedHolders;          // tx IDs currently holding shared lock
    int       exclusiveHolder = -1;   // tx ID holding exclusive lock (-1 = none)
};
```

```
  lockTable["student 5"]
  ┌──────────────────────────────────────┐
  │  type            = LOCK_SHARED       │
  │  sharedHolders   = {1, 2}            │
  │  exclusiveHolder = -1                │
  └──────────────────────────────────────┘

  lockTable["class 3"]
  ┌──────────────────────────────────────┐
  │  type            = LOCK_EXCLUSIVE    │
  │  sharedHolders   = {}                │
  │  exclusiveHolder = 3                 │
  └──────────────────────────────────────┘
```

**Lock type hierarchy:**

```
  LOCK_NONE  ──► compatible with any request
  LOCK_SHARED ──► compatible with other shared requests only
  LOCK_EXCLUSIVE ──► incompatible with all other requests
```

### 4.2 Transaction State

```cpp
map<int, Transaction> txMap;
```

Each `Transaction` tracks:

```cpp
struct Transaction {
    int      id;
    int      timestamp;          // logical start time; used by wait-die
    TxStatus status;             // TX_ACTIVE | TX_ABORTED | TX_COMMITTED

    // Locks held in ACQUISITION ORDER (crucial for correct unlock sequencing)
    vector<pair<string, LockType>> locksHeld;
};
```

**Why `vector` instead of `map` for `locksHeld`?**

Unlock messages must appear in **acquisition order** (the order the locks were taken), not alphabetical order. Using a `vector<pair<>>` preserves insertion order. For example, if T1 first reads `student 5` and then writes `class 3`, the commit output is:

```
COMMIT T1
unlock(student 5) by T1     ← acquired first
unlock(class 3) by T1       ← acquired second
```

### 4.3 Waiting and Restart Queues

```cpp
map<int, Op>  waiting;       // txID → the single op it is blocked on
set<int>      needsRestart;  // txIDs that were aborted and must be replayed
```

Only one op per transaction can be blocked at any time (a transaction waits on a single resource). `processPending()` scans `waiting` after any lock release and unblocks eligible transactions.

### 4.4 Operation Struct

```cpp
struct Op {
    string opType;    // "BEGIN" | "READ" | "WRITE" | "COMMIT"
    int    txID;
    string tableName;
    int    pageNum;
};
```

---

## 5. Algorithm — Strict 2PL with Wait-Die

### 5.1 Lock Compatibility Matrix

```
               Requested →    SHARED     EXCLUSIVE
Held ↓
  NONE                        GRANT      GRANT
  SHARED (others)             GRANT      wait-die check
  SHARED (self only)          GRANT      UPGRADE (grant)
  EXCLUSIVE (self)            GRANT      GRANT
  EXCLUSIVE (other)           wait-die   wait-die
```

### 5.2 Wait-Die Rule

```
  Requester timestamp < Holder timestamp  (requester is OLDER)
      → Requester WAITS

  Requester timestamp > Holder timestamp  (requester is YOUNGER)
      → Requester DIES (aborted + rolled back)
```

This is a **deadlock prevention** scheme (not detection). Because a younger transaction always dies rather than waiting for an older one, circular waits are impossible — the timestamp order is strictly maintained.

### 5.3 Core Lock Acquisition Logic

#### Shared lock request (`acquireShared`)

```
  Request: Tx txID wants SHARED lock on page key

  if lockTable[key].type == NONE or SHARED:
      Grant  →  add txID to sharedHolders, record in locksHeld
               print "Lock granted"

  elif lockTable[key].type == EXCLUSIVE:
      holder = exclusiveHolder
      if txID is older than holder:
          print "T<id> waits (older than T<holder>, so allowed to wait)"
          return BLOCKED
      else:
          print "T<id> dies (younger than T<holder>, cannot wait)"
          abortTx(txID)
          return ABORTED
```

#### Exclusive lock request (`acquireExclusive`)

```
  Request: Tx txID wants EXCLUSIVE lock on page key

  if lockTable[key].type == NONE:
      Grant immediately

  if lockTable[key].type == EXCLUSIVE and holder == txID:
      Grant (already held)

  if lockTable[key].type == EXCLUSIVE and holder ≠ txID:
      wait-die check against holder

  if lockTable[key].type == SHARED:
      if sharedHolders == {txID}:          ← sole holder: UPGRADE
          Grant (shared → exclusive)
      else:
          for each holder h ≠ txID:
              if txID is YOUNGER than h → DIES (h is the blocker)
          if txID is OLDER than ALL holders → WAITS
```

### 5.4 Pending Unblock (`processPending`)

Called after every lock release (COMMIT or abort). Iterates the `waiting` map and tries to grant pending locks:

```
  repeat until no progress:
      for each (txID, pendingOp) in waiting:
          if canAcquireNow(txID, page, isWrite):
              grant lock
              execute op (print "WRITE/READ T<id> ...")
              remove from waiting
              progress = true
          else:
              re-check wait-die against current holders
              if txID must now die (younger than a current holder):
                  abortTx(txID)
                  add to needsRestart
                  remove from waiting
                  progress = true
              else:
                  leave in waiting (no message printed)
```

The repeat-until-no-progress loop is needed because unblocking one transaction may release locks and allow a second waiting transaction to proceed.

### 5.5 Main Schedule Processing Loop

```
  for each op in schedule:
      globalTime++

      if tx[op.txID].status == ABORTED:   skip (will replay on restart)
      if op.txID in waiting:              skip (processPending handles it)

      switch op.opType:
          BEGIN:
              create Transaction with timestamp = globalTime
              print "BEGIN T<id>"

          COMMIT:
              tx.status = COMMITTED
              print "COMMIT T<id>"
              releaseAllLocks(txID)      ← strict 2PL: all locks released at commit
              processPending()

          READ / WRITE:
              validate table + page exist
              attempt lock acquisition
              if granted:  execute op
              if blocked:  add to waiting, call processPending
              if aborted:  add to needsRestart, call processPending
```

### 5.6 Lock Release (`releaseAllLocks`)

Iterates `locksHeld` in acquisition order, producing one `unlock(...)` line per lock:

```
  for (key, lockType) in tx.locksHeld:    ← acquisition order guaranteed by vector
      if lockType == SHARED:
          remove txID from lockTable[key].sharedHolders
          if sharedHolders is now empty: set type = LOCK_NONE
      elif lockType == EXCLUSIVE:
          lockTable[key].exclusiveHolder = -1
          lockTable[key].type = LOCK_NONE
      print "unlock(<key>) by T<id>"

  tx.locksHeld.clear()
```

---

## 6. Detailed Walkthrough (Sample Test Case 1)

**Input:**
```
BEGIN T1        ← t=1, T1.ts=1
BEGIN T2        ← t=2, T2.ts=2
READ T1 student 5
READ T2 class 3
WRITE T1 class 3
WRITE T2 student 5
COMMIT T1
COMMIT T2
```

**Step-by-step execution:**

```
t=1  BEGIN T1     txMap[1] = {ts=1, ACTIVE}   → "BEGIN T1"
t=2  BEGIN T2     txMap[2] = {ts=2, ACTIVE}   → "BEGIN T2"

t=3  READ T1 student 5
     acquireShared(T1, "student 5")
     lockTable["student 5"].type = NONE  → GRANT
     locksHeld[T1] = [("student 5", SHARED)]
     → "T1 requests shared lock(student 5)"
     → "Lock granted"
     → "READ T1 student 5"

t=4  READ T2 class 3
     acquireShared(T2, "class 3")
     lockTable["class 3"].type = NONE  → GRANT
     locksHeld[T2] = [("class 3", SHARED)]
     → "T2 requests shared lock(class 3)"
     → "Lock granted"
     → "READ T2 class 3"

t=5  WRITE T1 class 3
     acquireExclusive(T1, "class 3")
     lockTable["class 3"].type = SHARED, sharedHolders = {2}
     sharedHolders ≠ {T1}  →  check wait-die vs T2
     T1.ts=1 < T2.ts=2  →  T1 is OLDER  →  WAIT
     waiting[1] = {WRITE, T1, "class 3", 3}
     → "T1 requests exclusive lock(class 3)"
     → "T1 waits (older than T2, so allowed to wait)"
     processPending(): T1 still blocked (T2 holds shared) → no change

t=6  WRITE T2 student 5
     T2 not in waiting  →  proceed
     acquireExclusive(T2, "student 5")
     lockTable["student 5"].type = SHARED, sharedHolders = {1}
     sharedHolders ≠ {T2}  →  check wait-die vs T1
     T2.ts=2 > T1.ts=1  →  T2 is YOUNGER  →  DIE
     abortTx(T2):
         txMap[2].status = ABORTED
         → "T2 requests exclusive lock(student 5)"
         → "T2 dies (younger than T1, cannot wait)"
         → "ABORT T2"  "ROLLBACK T2"
         releaseAllLocks(T2):
             locksHeld[T2] = [("class 3", SHARED)]
             lockTable["class 3"].sharedHolders.erase(2)  → now empty → LOCK_NONE
             → "unlock(class 3) by T2"
     needsRestart.insert(2)
     processPending():
         T1 waiting on "class 3" exclusive
         canAcquireNow(T1, "class 3", WRITE)?
         lockTable["class 3"].type = LOCK_NONE  → YES
         GRANT exclusive to T1
         locksHeld[T1] = [("student 5", SHARED), ("class 3", EXCLUSIVE)]
         → "T1 exclusive lock(class 3) granted"
         bufferManager.getPage("class", 3)
         → "WRITE T1 class 3"
         waiting.erase(T1)

t=7  COMMIT T1
     T1 not in waiting  →  proceed
     txMap[1].status = COMMITTED
     → "COMMIT T1"
     releaseAllLocks(T1):
         locksHeld[T1] in order: ("student 5", SHARED), ("class 3", EXCLUSIVE)
         → "unlock(student 5) by T1"
         → "unlock(class 3) by T1"

t=8  COMMIT T2
     txMap[2].status = ABORTED  →  skip

Schedule loop ends.
needsRestart = {2}

Restart T2:
     globalTime = 9, T2.ts = 9
     → "\nRestart T2"
     Replay original ops:
         READ T2 class 3:
             lockTable["class 3"].type = LOCK_NONE → GRANT
             → "T2 requests shared lock(class 3)"  "Lock granted"  "READ T2 class 3"
         WRITE T2 student 5:
             lockTable["student 5"].type = LOCK_NONE → GRANT exclusive
             → "T2 requests exclusive lock(student 5)"  "Lock granted"  "WRITE T2 student 5"
         COMMIT T2:
             → "COMMIT T2"
             locksHeld[T2] in order: ("class 3", SHARED), ("student 5", EXCLUSIVE)
             → "unlock(class 3) by T2"
             → "unlock(student 5) by T2"
```

**Full output:**
```
BEGIN T1
BEGIN T2
T1 requests shared lock(student 5)
Lock granted
READ T1 student 5
T2 requests shared lock(class 3)
Lock granted
READ T2 class 3
T1 requests exclusive lock(class 3)
T1 waits (older than T2, so allowed to wait)
T2 requests exclusive lock(student 5)
T2 dies (younger than T1, cannot wait)
ABORT T2
ROLLBACK T2
unlock(class 3) by T2
T1 exclusive lock(class 3) granted
WRITE T1 class 3
COMMIT T1
unlock(student 5) by T1
unlock(class 3) by T1

Restart T2
T2 requests shared lock(class 3)
Lock granted
READ T2 class 3
T2 requests exclusive lock(student 5)
Lock granted
WRITE T2 student 5
COMMIT T2
unlock(class 3) by T2
unlock(student 5) by T2
```

---

## 7. Detailed Walkthrough (Sample Test Case 2)

**Input:**
```
BEGIN T1           ← T1.ts=1
BEGIN T2           ← T2.ts=2
BEGIN T3           ← T3.ts=3
READ T1 A 20
READ T2 F 10
WRITE T3 A 20      ← T3 conflicts with T1 on A 20
WRITE T1 F 10      ← T1 conflicts with T2 on F 10
WRITE T2 A 20      ← T2 conflicts with T1 on A 20
COMMIT T1
COMMIT T2
COMMIT T3
```

**Key decisions:**

```
READ T1 A 20   → sharedHolders["A 20"] = {1}
READ T2 F 10   → sharedHolders["F 10"] = {2}

WRITE T3 A 20:
    sharedHolders["A 20"] = {1}  (T1 holds shared)
    wait-die: T3.ts=3 > T1.ts=1  →  T3 YOUNGER  →  DIES
    locksHeld[T3] is EMPTY  →  no unlock messages printed
    → "T3 requests exclusive lock(A 20)"
    → "T3 dies (younger than T1, cannot wait)"
    → "ABORT T3"  "ROLLBACK T3"

WRITE T1 F 10:
    sharedHolders["F 10"] = {2}  (T2 holds shared)
    wait-die: T1.ts=1 < T2.ts=2  →  T1 OLDER  →  WAITS
    → "T1 waits (older than T2, so allowed to wait)"
    processPending(): T1 still blocked (T2 holds F 10) → no change

WRITE T2 A 20:
    sharedHolders["A 20"] = {1}  (T1 holds shared)
    wait-die: T2.ts=2 > T1.ts=1  →  T2 YOUNGER  →  DIES
    abortTx(T2): locksHeld[T2] = [("F 10", SHARED)]
    → "unlock(F 10) by T2"
    needsRestart.insert(2)
    processPending():
        T1 waiting on exclusive "F 10"
        lockTable["F 10"].type = LOCK_NONE (T2 released)  →  GRANT T1
        → "T1 exclusive lock(F 10) granted"
        → "WRITE T1 F 10"

COMMIT T1:
    locksHeld[T1] in order: ("A 20", SHARED), ("F 10", EXCLUSIVE)
    → "unlock(A 20) by T1"
    → "unlock(F 10) by T1"

Restart T2 (ts=12), Restart T3 (ts=13):
    Both replay without conflict (all prior locks released).
```

**Full output matches expected** — T3 dies with no unlocks, unlock order follows acquisition order, restarts proceed cleanly.

---

## 8. Restart Mechanism

Aborted transactions are replayed **in original-timestamp order** (oldest-first). This ensures that higher-priority (older) transactions get their locks before lower-priority ones, preventing starvation.

```
  After main schedule loop completes:

  while needsRestart not empty:
      target = tx with smallest timestamp in needsRestart
      needsRestart.erase(target)
      globalTime++
      assign new timestamp (globalTime) to target
      print "\nRestart T<target>"

      replay original ops (skip BEGIN):
          for each op in originalOps[target]:
              READ/WRITE: acquireShared/acquireExclusive
                          (retry loop in case of momentary block)
              COMMIT:     releaseAllLocks, processPending, return
```

**Why restarted transactions succeed without dying again:**

A restarted transaction receives the highest timestamp (most recent `globalTime`). It is therefore always the "youngest" and, under wait-die, would die if it conflicts with an older active transaction. However, at restart time all preceding transactions have either committed (locks released) or not yet restarted (so they hold no locks). Therefore, a restarted transaction can always acquire its locks immediately.

If two aborted transactions are restarted sequentially (e.g., T2 before T3), T2 completes and releases its locks before T3 begins, so no conflict arises.

---

## 9. Integration with SimpleRA

### 9.1 Program Flow Preservation

The overall command processing pipeline (`server.cpp → syntacticParser → semanticParser → executor`) is unchanged. The `TRANSACTION` command is dispatched exactly like all other commands via the `executeCommand()` switch statement.

### 9.2 Cursor and BufferManager Usage

As required, actual page accesses go through `bufferManager.getPage(tableName, pageIndex)` whenever a lock is granted for a READ or WRITE operation. This ensures `BLOCK_ACCESSES` is incremented and the buffer pool is exercised, even though page contents are not modified.

```cpp
// In runOp() and processPending():
bufferManager.getPage(op.tableName, op.pageNum);   // actual I/O call
out << op.opType << " T" << id << " " << ...;      // log the operation
```

### 9.3 Table and Page Validation

Before attempting any lock, the implementation validates:

1. The table exists in the `TableCatalogue` via `tableCatalogue.isTable(tableName)`
2. The page number is within range: `0 ≤ pageNum < table->blockCount`

Errors are written both to the output file and to stdout:
```
SEMANTIC ERROR: Relation doesn't exist
SEMANTIC ERROR: Page doesn't exist
```

### 9.4 Page Key Convention

The internal lock-table key uses the format `"<tableName> <pageNumber>"` (e.g., `"student 5"`). This is also the display format in the output log, so lock request and unlock messages directly use the key string — no separate formatting step is needed.

### 9.5 Files Added / Modified

| File | Change |
|---|---|
| `src/syntacticParser.h` | Added `TRANSACTION` to `QueryType` enum; added `transactionFileName` to `ParsedQuery`; declared `syntacticParseTRANSACTION()` |
| `src/syntacticParser.cpp` | Added dispatcher entry; `transactionFileName` reset in `ParsedQuery::clear()`; `syntacticParseTRANSACTION()` implementation |
| `src/semanticParser.h` | Declared `semanticParseTRANSACTION()` |
| `src/semanticParser.cpp` | Added switch case; `semanticParseTRANSACTION()` checks file existence via `stat()` |
| `src/executor.h` | Declared `executeTRANSACTION()` |
| `src/executor.cpp` | Added `case TRANSACTION: executeTRANSACTION()` |
| `src/executors/transaction.cpp` | **New file** — full S2PL + wait-die implementation |
| `src/Makefile` | Changed `CXX = g++` to `CXX = g++-15` (required on macOS; `bits/stdc++.h` unavailable in Apple Clang) |

---

## 10. Error Handling

| Condition | Output |
|---|---|
| Schedule file not found | `SEMANTIC ERROR: Transaction schedule file doesn't exist` (printed to stdout before execution; command is rejected) |
| Table does not exist | `SEMANTIC ERROR: Relation doesn't exist (<table>)` written to output file |
| Page number out of range | `SEMANTIC ERROR: Page doesn't exist (<table> page <n>)` written to output file |
| Transaction is younger and conflicts | `ABORT T<n>` + `ROLLBACK T<n>` + unlock messages |
| Transaction is older and conflicts | Wait state; unblocked when conflicting transaction releases |

---

## Assumptions

- **Simulation only.** The implementation does not modify actual page data. `bufferManager.getPage()` is called as required to maintain correct `BLOCK_ACCESSES` tracking, but the returned page contents are ignored.

- **Sequential processing.** All operations in the schedule are processed in the order given. There is no actual concurrency (no threads). The simulation assumes each operation takes 1 unit of time.

- **Timestamp assignment.** The first `BEGIN` receives timestamp 1. Every subsequent operation (BEGIN, READ, WRITE, COMMIT) increments the global logical clock by 1, matching the specification: *"the first transaction begins at t=1 and each step takes 1 unit of time."*

- **Restart timestamps.** Restarted transactions receive a new timestamp equal to the current `globalTime` at the moment of restart. This guarantees restarted transactions are always "younger" than original transactions in terms of timestamp ordering, but since all prior transactions have committed by restart time, no conflicts arise.

- **Restart order.** When multiple transactions need to be restarted, they are replayed in **original timestamp order** (smallest original timestamp first). This prevents starvation and mirrors the order in which they originally began.

- **Wait-die is applied per conflicting holder.** When a transaction requests an exclusive lock on a page with multiple shared holders, wait-die is evaluated against all holders. If the requester is younger than **any** holder, it dies; only if it is older than **all** holders does it wait.

- **Lock upgrade.** A transaction holding a shared lock on a page may upgrade to exclusive if and only if it is the **sole** shared holder. If other transactions also hold shared locks on the same page, the upgrade is treated as a fresh exclusive request and subject to wait-die.

- **No partial rollback.** When a transaction aborts, all its locks are released immediately. The rollback is conceptual (no data was written), so no undo logging is needed.

- **Tables must be loaded before TRANSACTION.** The semantic parser validates table and page existence using `tableCatalogue`, which only contains tables loaded via the `LOAD` command in the current session. If a transaction references a table that has not been loaded, a semantic error is logged.

- **BLOCK_SIZE and BLOCK_COUNT are unchanged** at their default values (1 KB and 2 respectively). The implementation does not modify either constant.

- **Private class members are not accessed directly.** All interactions with `Table`, `Page`, `Cursor`, and `BufferManager` use their existing public API.

---

## Team Contributions

---
