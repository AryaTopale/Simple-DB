# SimpleRA Database Engine Project (Phases 1–3)

## Overview

SimpleRA is a lightweight relational database engine developed incrementally across three academic project phases for the **Data Systems** course. The system supports relational algebra style query execution, disk-page based storage, buffering, and transaction concurrency control.

This repository contains the cumulative implementation from **Phase 1**, **Phase 2**, and **Phase 3**.

---

## Project Goals

* Understand internal architecture of DBMS systems
* Implement core relational operators from scratch
* Work with page-based storage and buffer management
* Build query parsing and execution pipelines
* Simulate concurrency control protocols used in real DBMSs

---

## Architecture

```text
User Command
   ↓
Tokenizer
   ↓
Syntactic Parser
   ↓
Semantic Parser
   ↓
Executor
   ↓
Table / Buffer / Cursor / Storage Layer
```

---

## Phase 1 — Core Relational Engine

Implemented foundational database functionality:

### Features

* CSV table loading into internal storage
* Page/block based table storage
* Catalog management for loaded tables
* Basic query parser and command execution
* Projection, selection, sorting, ordering
* Export / print operations
* Cursor-based page scanning
* Buffer manager integration

### Example Commands

```sql
LOAD student
PRINT student
SELECT * FROM student
PROJECT name,age FROM student
```

---

## Phase 2 — Advanced Query Processing

Extended engine with more relational and analytical operations.

### Features

* Join operations
* Group By / Aggregation
* Set operations (UNION / INTERSECTION / DIFFERENCE)
* External sorting enhancements
* Query optimization improvements
* Additional parser and executor modules

### Example Commands

```sql
R <- JOIN student, marks ON id
S <- GROUP BY dept FROM employee RETURN COUNT(id)
```

---

## Phase 3 — Concurrency Control

Implemented page-level transaction scheduling as a simulation layer.

### Protocol Implemented

* **Strict Two-Phase Locking (S2PL)**
* **Wait-Die deadlock prevention**

### Input Command

```sql
TRANSACTION schedule1.txt
```

### Supported Schedule Operations

```text
BEGIN T1
READ T1 student 3
WRITE T1 student 3
COMMIT T1
```

### Features

* Shared / Exclusive page locks
* Lock upgrade handling
* Wait / Abort decisions using timestamps
* Rollback and restart simulation
* Execution log generation
* Uses existing buffer manager flow

---

## Folder Structure

```text
src/
  parser/
  executors/
  bufferManager/
  table/
  transaction.cpp

data/
  input tables
  transaction schedules
  generated outputs

docs/
  reports / documentation
```

---

## Build Instructions

```bash
make
./simpleRA
```

If using macOS / newer GCC:

```bash
make CXX=g++-15
```

---

## Sample Workflow

```sql
LOAD student
LOAD marks
R <- JOIN student, marks ON id
TRANSACTION schedule1.txt
```

---

## Key Concepts Used

* Relational Algebra
* External Memory Algorithms
* Buffer Management
* Page Layouts
* Query Parsing
* Concurrency Control
* Deadlock Prevention
* Transaction Scheduling

---

## Learning Outcomes

This project provided hands-on understanding of how database systems internally process queries, manage storage, and enforce correctness under concurrent transactions.

---

## Future Improvements

* Cost-based query optimizer
* Indexing (B+ Trees / Hash)
* Recovery logging (WAL)
* MVCC support
* SQL frontend parser
* Real parallel execution

---

## Course

Data Systems — Spring 2026
