# Project Report — Graph Support for SimpleRA

<b>Team 10 : Divyansh Pandey (2022101111), Hemang Jain (2022101086), Arya Topale (2022102052)</b>

## Overview of key design choices

- Attribute masks: attributes for nodes and edges are represented as a compact bit-packed mask using 8 integers (MASK_INT_COUNT = 8). Each int is 32 bits → 8 * 32 = 256 bits to cover up to 250 attributes. Bitwise operations are used to check attributes quickly.
- Page-oriented tables: each logical table (NodesGraph, EdgesGraph, IndexGraph, DegreeGraph, EdgesOriginalGraph) is implemented as a table with fixed number of integer columns. We have Degree Graph for fast degree lookup in static graph, and original edge graph for PRINT ordering. Pages / blocks are filled up to `maxRowsPerBlock` and flushed as pages. This enables predictable block accesses.
- Indexing for adjacency: neighbors are accessed using a small index table that stores {src_id, start_blk, start_off, cnt} sorted by src_id. The index table is searched using binary search on block-level summaries and then scanned within a single block to retrieve neighbor edge entries. This keeps block accesses to O(log B) + O(1) for a node lookup.
<img src="page.png"></img>
<i>Nano Banana image for reference, slight generation mistakes</i>
- Two-block memory constraint respected in edge loading: a three-pass loader counts edges per source, computes positions, and streams edges to disk. This design keeps memory usage low while producing contiguous blocks of edges per source for efficient adjacency scans.

---

## Command: LOAD GRAPH


What it does

- `LOAD GRAPH <graph_name> <U|D>`: creates a `Graph` object and loads it as blocks in /data/tmp directory. `syntacticParseLOAD_GRAPH` performs syntax checks and stores the graph type. `semanticParseLOAD_GRAPH` ensures a graph with that name does not already exist and that the expected node/edge CSV files exist. `executeLOAD_GRAPH` constructs `Graph` and calls `graph->load()`.

Implementation logic (Graph load)
- `Graph::Graph(...)` initializes a `GraphStorage` and `Table` wrappers for node/edge tables. `Graph::load()` delegates to `GraphStorage::loadNodesFromCsv()` and `GraphStorage::loadEdgesAndBuildIndex()`.
- `GraphStorage::loadNodesFromCsv(path)` reads the node CSV header and rows, parses attribute columns into an 8-int mask using `parseAttributeMask`, and writes node table pages. A node table row layout is: [node_id, mask0, mask1, ..., mask7]. Pages are batches of `nodeTable->maxRowsPerBlock` rows which are flushed to disk (table catalogue) when full.
- `GraphStorage::loadEdgesAndBuildIndex(path)` is a three-pass loader:
  1. Pass 0 (store original): read the edge CSV, parse attributes into masks and store them in an `EdgesOriginalGraph` table in the original order. This preserves original edges and attributes.
  2. Pass 1 (count): stream the CSV again and count edges per source and per destination (used later to create the degree table and to compute positions).
  3. Pass 2 (index calc) & Pass 3 (stream sorted): compute cumulative positions to determine where each source's edges will be written in the final `EdgesGraph` table. Then stream edges into `EdgesGraph` writing contiguous edge groups for each source. While streaming, the loader fills the index table `IndexGraph` rows with {src_id, start_blk, start_off, cnt}.

Page design (tables used in load)
- Node table columns: `node_id` + 8 mask ints → total columns = 1 + MASK_INT_COUNT
- Edge table columns (final): `src`, `dst`, `w` + 8 mask ints → total columns = 3 + MASK_INT_COUNT
- Edges original table: same layout as final edge table; kept for preservation or other operations
- Index table: columns `{src_id, start_blk, start_off, cnt}`
- Degree table: `{node_id, in_degree, out_degree, total_degree}`

Block access / complexity during load
- Loading is I/O heavy by nature; the loader streams files once or more depending on the pass. The three-pass design trades extra scanning of the CSV for small memory footprint. Memory used is dominated by per-source counters and the final index metadata; this is kept small (maps keyed by source id) and does not keep all edges in memory.

Error handling
- File open failures immediately return false and propagate up: `loadNodesFromCsv` and `loadEdgesAndBuildIndex` return false on errors. `syntacticParseLOAD_GRAPH` and `semanticParseLOAD_GRAPH` validate inputs and file existence and print `SYNTAX ERROR` or `SEMANTIC ERROR` when appropriate.

---

## Command: DEGREE

Relevant file: `degree.cpp`, `graphstorage.cpp` (getDegreeInfo)

What it does
- `DEGREE <graph_name> <node_id>`: Query precomputed degree table for a node. The degree table is populated during edge load.

Implementation logic
- `syntacticParseDEGREE` validates tokens and parses the node id.
- `semanticParseDEGREE` checks that the graph exists and node id is non-negative.
- `executeDEGREE` calls `graph->storage->getDegreeInfo(nodeID)`. That routine performs a binary-search-on-blocks over the degree table to find the block containing the node, then scans within that block to find the row with the node id. If not found, an empty vector is returned and the executor reports "Node does not exist".

Page design and block access
- Degree table rows: `[node_id, in_degree, out_degree, total_degree]`.
- `getDegreeInfo` uses O(log B) block-level binary search (B = number of blocks) and then one block read for the target block followed by an in-block linear scan. So block accesses ≈ O(log B) + 1.

Error handling
- If `getDegreeInfo` returns empty, `executeDEGREE` prints "Node does not exist". Semantic errors are raised earlier (graph missing, negative ID).

---

## Command: PATH

Relevant file: `path.cpp`, supporting: `graphstorage.cpp`, `graph.cpp`

Synopsis (syntax)
- Expected syntax: `RES <- PATH graph_name srcNodeID destNodeID [WHERE <conditions> AND ...]`.

What it does (high level)
- Finds a (shortest-weight) path between src and dest that satisfies node/edge attribute conditions. Supports simple attribute conditions and the special `ANY(N)` style condition (uniform attribute requirement across nodes in path). The algorithm uses Dijkstra's algorithm with attribute filters applied at neighbor expansion and with careful caching of node/edge masks to minimize repeated block reads.

Implementation details
- Attribute masks and helpers:
  - `MASK_INT_COUNT = 8` (same as storage) used to interpret masks read from tables.
  - `getAttributeValue(masks, attrName)` extracts the bit value (0/1) for a named attribute (like "A3", "B10"). It computes index and then selects the right mask integer and bit position.
  - `isBitSet(masks, bitIndex)` is a lower-level helper to check a specific bit index (0-249).
  - `nodeMatchesConditions(nodeMasks, conditions)` and `edgeMatchesConditions(edgeMasks, conditions)` encapsulate condition checking for nodes and edges respectively.

- Graph traversal and Dijkstra:
  - `executePATH()` obtains `Graph* graph` from the catalogue and retrieves source and destination masks via `getNodeMask(graph->storage, id)` which calls `GraphStorage::getNodeMaskById`.
  - If either mask is empty, the node does not exist and the command prints `FALSE` (or a semantic error message) and returns.
  - It first checks that the endpoints satisfy node-related conditions before running search.
  - Dijkstra (priority queue of (distance, nodeID)) is used to find the minimum-weight path. Data structures used:
    - `dist` map: nodeID -> best known distance.
    - `parent` map: nodeID -> parent nodeID (used to reconstruct path).
    - `nodeMasks` unordered_map caches node masks already read from storage, so the same node's mask isn't read from disk multiple times.
  - For each node popped from pq, `getNeighbors` (via `GraphStorage::getNeighbors`) returns neighbor edge rows for that source. The loader reads edge masks (edgeMasks) on demand and uses `edgeMatchesConditions` to decide whether to relax edges.

- After finishing search and confirming a path exists, the path is reconstructed using `reconstructPath(parent, src, dest)` and uniform/ANY conditions are re-checked across the whole path (some conditions like `ANY(N)` require checking that the same attribute bit is uniform along the path nodes or edges; this validation is applied post-hoc to ensure correctness).

Page design and index usage (neighbor access)
- Node table layout: `[node_id, mask0..mask7]` as above. Node lookups use binary search on node table blocks (block-level binary search followed by an in-block scan) implemented in `GraphStorage::getNodeMaskById` so typical node retrieval cost is O(log B) block accesses + 1 target block read.
- Edge adjacency layout: `EdgesGraph` stores edges sorted by source. `IndexGraph` provides per-source metadata with the starting block and offset plus count. `GraphStorage::getNeighbors` finds which block contains the source's index entry using binary search on the index table blocks, then scans only that block to get the `start_blk/start_off/cnt` entry and then reads the corresponding edge pages.

Block access complexity during PATH
- Endpoint mask retrieval: 2 × (O(log B) + 1) block accesses for source and destination masks.
- During Dijkstra: for each expanded node, neighbors are fetched by reading one index block (binary-search cost ≈ O(log BI) where BI is number of index blocks) then reading the contiguous edge pages identified by the index row. Because edges of a single source are stored contiguously, fetching neighbors requires reading only the blocks covered by that node's adjacency list (often 1 or a small number of blocks). The binary search for index entry is O(log B_index) + 1 block, then reading the neighbor edge blocks as needed.
- Node mask caching prevents re-reading the same node mask multiple times; each node's mask is read at most once during the search.

Error handling and semantic checks
- Syntactic checks in `syntacticParsePATH` verify token positions, integers for src/dest, presence/format of WHERE clause and condition tokens.
- `semanticParsePATH` ensures result relation does not already exist, graph exists, and node IDs are non-negative.
- `executePATH` handles missing nodes (empty masks) by returning failure early. It also validates that source/destination satisfy node-related conditions before searching.

Notes on correctness and safety
- Because `nodeMasks` are cached and only masks are cached (not full node rows), memory usage is proportional to the number of nodes touched by the search rather than the whole graph.
- The design assumes edge weights are integers and non-negative so Dijkstra is correct. If negative weights are possible, the code would need a different algorithm.

---

## Justification for the chosen page/table design

The primary goals were: minimal block accesses for common operations (neighbor lookup, node attribute lookup), a compact representation for a potentially large set of attributes, and low memory usage during loading.

- Bit-packed masks (8 ints): storing attributes as multiple 32-bit integers is compact and very fast for attribute queries. Bit tests are single-instruction operations and avoid string parsing at query time. The 8-int layout gives a fixed-width row which improves page packing and simplifies persistence and I/O logic.
- Separate index table with {src_id, start_blk, start_off, cnt}: by sorting edges by source and writing contiguous blocks for each source we enable retrieving all neighbors of a node by reading a small, contiguous portion of the edge file. The index table being compact and sorted allows O(log B) block-level search and then O(1) in-block scan. This design balances fast adjacency access with the 2-block memory constraint during build.
- Degree table: precomputing degrees during load means `DEGREE` queries are fast and do not require scanning edges on demand.


---

## Assumptions

1. CSV file paths: loaders expect files under `../data/` relative to the binary or working directory. For `LOAD GRAPH G D` the code expects `../data/G_Nodes_D.csv` and `../data/G_Edges_D.csv` to exist.
2. Attribute header format: node and edge attribute columns follow the header conventions such that `parseAttributeMask` can split on commas and map attributes sequentially into bit positions 0..(attrCount-1), up to 250 attributes. The implementation maps the i-th attribute column to bit i.
3. Masks use little-endian ordering within each 32-bit integer bitfield (bit 0 is the least-significant bit in mask integer 0). `isBitSet` and `getAttributeValue` follow that convention.
4. Table and buffer manager semantics: `Table->maxRowsPerBlock` and `bufferManager.getPage(tableName, block)` exist and provide page/block-level access and row counts per block. 
5. Index and node tables fit the available integer-based columns and the project's Table abstraction supports tables with these fixed integer columns.
6. Attribute names in PATH conditions: attributes are referenced as e.g., `A1`, `B3`. `getAttributeValue` expects a single leading character category (A/B) and a numeric suffix indicating the attribute index; invalid formats are treated as syntax/semantic errors.
7. Edge weights are non-negative integers so Dijkstra is appropriate.

---



