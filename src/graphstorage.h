#pragma once
#include "bufferManager.h"
#include "cursor.h"
#include <string>
#include <vector>
typedef unsigned int uint;
class GraphStorage {
public:
    /**
     * @brief Construct a new Graph Storage object
     * @param graphName Name of the graph (used for table naming)
     * @param directed True if D, False if U [cite: 75]
     */
    GraphStorage(const std::string &graphName, bool directed);

    /**
     * @brief Loads nodes from CSV and builds a bitmask of attributes [cite: 42, 45]
     * @return true on success, false otherwise
     */
    bool loadNodesFromCsv(const std::string &nodeCsvPath);

    /**
     * @brief Loads edges from CSV and builds an External Adjacency Index [cite: 48, 52]
     * This method maps Src_NodeID to its physical PageID and Offset.
     * @return true on success, false otherwise
     */
    bool loadEdgesAndBuildIndex(const std::string &edgeCsvPath);
    
    /**
     * @brief Retrieves neighbors using the External Adjacency Index.
     * Minimizes block access by jumping directly to edge locations[cite: 90].
     * @return Vector of edge rows: {src, dest, weight, attr_mask}
     */
    std::vector<std::vector<int>> getNeighbors(int srcNodeID);

    /**
     * @brief Get degree information for a node
     * @param nodeID The node to query
     * @return Vector with [in_degree, out_degree, total_degree], empty if node not found
     */
    std::vector<int> getDegreeInfo(int nodeID);

    /**
     * @brief Get node attribute masks using binary search
     * @param nodeID The node to query
     * @return Vector of 8 mask integers, empty if node not found
     */
    std::vector<int> getNodeMaskById(int nodeID);

    // Table names for internal management
    std::string nodeTableName;
    std::string nodeTableNameSorted; // Sorted version for binary search
    std::string edgeTableName;
    std::string indexTableName; // The Adjacency Index Table
    std::string degreeTableName; // Maps node_id -> (in_degree, out_degree, total_degree)

    unordered_map<int,int> startPage;
    unordered_map<int,int> pageCount;
    unordered_map<int,int> writePos;
    int adjRowsPerBlock;
    string edgeAdjTableName;
    string edgeOriginalTableName;
    int edgeAttributeCount = 0;
    int nodeAttributeCount = 0; // Track max node attribute index for CSV header generation
    int originalEdgeCount = 0;


    // Required counts for PRINT GRAPH and metadata [cite: 77, 184]
    int nodeRowcount = 0;
    int edgeRowcount = 0;

    // Cursors for full scans (used in PRINT GRAPH or EXPORT) [cite: 17, 181, 92]
    Cursor getNodeCursor();
    Cursor getEdgeCursor();

    // Metadata accessors
    std::string getGraphName() const { return _graphName; }
    bool isDirected() const { return _directed; }

private:
    std::string _graphName;
    bool _directed;

    // Internal tracking for block counts [cite: 20]
    uint _nodeBlockCount = 0;
    uint _edgeBlockCount = 0;
    uint _indexBlockCount = 0;
};