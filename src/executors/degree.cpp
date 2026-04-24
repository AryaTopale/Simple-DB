#include "global.h"
#include "graphCatalogue.h"

/**
 * @brief Syntactic parser for DEGREE command
 * Syntax: DEGREE <graph_name> <node_id>
 * Example: DEGREE G 3
 */
bool syntacticParseDEGREE()
{
    logger.log("syntacticParseDEGREE");
    
    // Expected: DEGREE <graph_name> <node_id>
    if (tokenizedQuery.size() != 3)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    
    parsedQuery.queryType = DEGREE;
    parsedQuery.degreeGraphName = tokenizedQuery[1];
    
    try {
        parsedQuery.degreeNodeID = stoi(tokenizedQuery[2]);
    } catch (...) {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    
    return true;
}

/**
 * @brief Semantic parser for DEGREE command
 * Validates that graph exists and node ID is valid
 */
bool semanticParseDEGREE()
{
    logger.log("semanticParseDEGREE");
    
    // Check graph exists
    if (!graphCatalogue.isGraph(parsedQuery.degreeGraphName))
    {
        cout << "SEMANTIC ERROR: Graph doesn't exist" << endl;
        return false;
    }
    
    // Validate node ID is non-negative
    if (parsedQuery.degreeNodeID < 0)
    {
        cout << "SEMANTIC ERROR: Invalid node id" << endl;
        return false;
    }
    
    return true;
}

/**
 * @brief Execute DEGREE command using precomputed degree table
 * Computes degree efficiently using O(N) lookup where N = number of nodes
 */
void executeDEGREE()
{
    logger.log("executeDEGREE");
    
    string graphName = parsedQuery.degreeGraphName;
    int nodeID = parsedQuery.degreeNodeID;
    BLOCK_ACCESSES = 0;
    Graph* graph = graphCatalogue.getGraph(graphName);
    
    // Query the degree table for this node
    vector<int> degreeInfo = graph->storage->getDegreeInfo(nodeID);
    
    if (degreeInfo.empty())
    {
        // Node not found in degree table
        cout << "Node does not exist" << endl;
        return;
    }
    
    // degreeInfo = [in_degree, out_degree, total_degree]
    int inDegree = degreeInfo[0];
    int outDegree = degreeInfo[1];
    int totalDegree = degreeInfo[2];
    
    // For undirected graphs, we just report total_degree
    // For directed graphs, total_degree = in_degree + out_degree
    if (graph->directed)
        cout << totalDegree << endl;
    else
        cout << totalDegree / 2 << endl;
    // cout << "BLOCK ACCESSES: " << BLOCK_ACCESSES << endl;
}
