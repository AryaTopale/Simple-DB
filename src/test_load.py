import os
import csv
import random
import heapq
from collections import defaultdict

DATA_DIR = "./data"
TEST_DIR = "./tests"

random.seed(42)

# ------------------ Utilities ------------------

def write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

# ------------------ Graph Generation ------------------

def gen_graph(name, n, m, directed):
    nodes = []
    edges = []

    for i in range(1, n + 1):
        nodes.append([i, random.randint(0, 1), random.randint(0, 1)])

    seen = set()
    while len(edges) < m:
        u = random.randint(1, n)
        v = random.randint(1, n)
        if u == v or (u, v) in seen:
            continue
        seen.add((u, v))
        edges.append([u, v, random.randint(1, 20),
                      random.randint(0, 1),
                      random.randint(0, 1)])

    suffix = "D" if directed else "U"

    write_csv(
        f"{DATA_DIR}/{name} Nodes {suffix}.csv",
        ["NodeID", "A1", "A2"],
        nodes
    )

    write_csv(
        f"{DATA_DIR}/{name} Edges {suffix}.csv",
        ["Src_NodeID", "Dest_NodeID", "Weight", "B1", "B2"],
        edges
    )

    return nodes, edges

# ------------------ Oracle (Dijkstra + filters) ------------------

def build_adj(edges, directed):
    adj = defaultdict(list)
    for u, v, w, b1, b2 in edges:
        adj[u].append((v, w, b1, b2))
        if not directed:
            adj[v].append((u, w, b1, b2))
    return adj

def dijkstra(adj, nodes, src, dst, node_cond=None, edge_cond=None):
    if node_cond and not node_cond(nodes[src]):
        return None

    pq = [(0, src)]
    dist = {src: 0}

    while pq:
        d, u = heapq.heappop(pq)
        if u == dst:
            return d
        if d > dist[u]:
            continue
        for v, w, b1, b2 in adj[u]:
            if node_cond and not node_cond(nodes[v]):
                continue
            if edge_cond and not edge_cond(b1, b2):
                continue
            nd = d + w
            if v not in dist or nd < dist[v]:
                dist[v] = nd
                heapq.heappush(pq, (nd, v))
    return None

# ------------------ Main Test Harness ------------------

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(TEST_DIR, exist_ok=True)

    commands = []
    expected = []

    # -------- Large Directed --------
    nodes_d, edges_d = gen_graph("LD", 5000, 20000, True)
    adj_d = build_adj(edges_d, True)
    node_map_d = {n[0]: n for n in nodes_d}

    commands.append("LOAD GRAPH LD D")
    expected.append("Loaded Graph.Node Count:5000,Edge Count:20000")

    # DEGREE sanity
    commands.append("DEGREE LD 1")
    deg = sum(1 for e in edges_d if e[0] == 1 or e[1] == 1)
    expected.append(str(deg))

    # PATH no WHERE
    commands.append("R <- PATH LD 1 5000")
    d = dijkstra(adj_d, node_map_d, 1, 5000)
    expected.append("FALSE" if d is None else f"TRUE {d}")

    # PATH with node filter
    commands.append("R <- PATH LD 1 5000 WHERE A1(N) == 1")
    d = dijkstra(
        adj_d, node_map_d, 1, 5000,
        node_cond=lambda n: n[1] == 1
    )
    expected.append("FALSE" if d is None else f"TRUE {d}")

    # -------- Large Undirected --------
    nodes_u, edges_u = gen_graph("LF", 500, 200000, True)
    adj_u = build_adj(edges_u, True)
    node_map_u = {n[0]: n for n in nodes_u}

    commands.append("LOAD GRAPH LF D")
    expected.append("Loaded Graph.Node Count:50000,Edge Count:200000")

    commands.append("R <- PATH LF 10 400")
    d = dijkstra(adj_u, node_map_u, 10, 400)
    expected.append("FALSE" if d is None else f"TRUE {d}")

    # -------- Edge Case: No Path --------
    commands.append("R <- PATH LL 1 9999")
    expected.append("FALSE")

    # -------- Write outputs --------
    with open(f"{TEST_DIR}/commands.txt", "w") as f:
        f.write("\n".join(commands))

    with open(f"{TEST_DIR}/expected.txt", "w") as f:
        f.write("\n".join(expected))

    print("✔ Stress test generated")
    print("Run:")
    print("cat tests/commands.txt | ./SimpleRA > my_output.txt")
    print("diff my_output.txt tests/expected.txt")

if __name__ == "__main__":
    main()
