#include "global.h"
#include "graphCatalogue.h"
#include <queue>
#include <unordered_map>
#include <limits>
#include <algorithm>

/**
 * @brief
 * SYNTAX (expected): R <- PATH graph_name srcNodeID destNodeID [WHERE <conditions>]
 *
 * This file contains the syntactic/semantic/execute skeleton for the PATH
 * executor. Updated to support 250-bit attribute masks stored as 8 integers.
 */

// Configuration: Must match graphstorage.cpp
const int MASK_INT_COUNT = 8; // 8 * 32 = 256 bits (covers 250 attributes)

vector<PathCondition> pathConditions;


bool syntacticParsePATH()
{
    logger.log("syntacticParsePATH");

    /*
        Expected forms:

        RES <- PATH G src dest
        RES <- PATH G src dest WHERE <cond> AND <cond> ...
    */

    int n = tokenizedQuery.size();

    // Minimum tokens: RES <- PATH G src dest
    if (n < 6)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Fixed keyword positions
    if (tokenizedQuery[1] != "<-" ||
        tokenizedQuery[2] != "PATH")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = PATH;
    parsedQuery.pathResultRelationName = tokenizedQuery[0];
    parsedQuery.pathGraphName = tokenizedQuery[3];

    // Parse src and dest
    try
    {
        parsedQuery.pathSrcNodeID  = stoi(tokenizedQuery[4]);
        parsedQuery.pathDestNodeID = stoi(tokenizedQuery[5]);
    }
    catch (...)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.pathConditions.clear();

    // No WHERE clause
    if (n == 6)
        return true;

    // WHERE must be exactly at index 6
    if (tokenizedQuery[6] != "WHERE")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    int i = 7;

    // Parse one or more conditions
    while (i < n)
    {
        PathCondition cond;
        cond.value = -1; // default = uniform

        /*
            Condition token formats:
            A3(N)
            B2(E)
            ANY(N)
        */

        string t = tokenizedQuery[i];

        int l = t.find('(');
        int r = t.find(')');

        if (l == string::npos || r == string::npos || r != t.length() - 1)
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }

        cond.attribute = t.substr(0, l);

        if (r != l + 2)
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }

        cond.appliesTo = t[l + 1];
        if (cond.appliesTo != 'N' && cond.appliesTo != 'E')
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }

        i++;

        // Optional: == 0 / == 1
        if (i < n && tokenizedQuery[i] == "==")
        {
            if (i + 1 >= n ||
                (tokenizedQuery[i + 1] != "0" &&
                 tokenizedQuery[i + 1] != "1"))
            {
                cout << "SYNTAX ERROR" << endl;
                return false;
            }

            cond.value = stoi(tokenizedQuery[i + 1]);
            i += 2;
        }

        parsedQuery.pathConditions.push_back(cond);

        // Either end OR AND <next-condition>
        if (i < n)
        {
            if (tokenizedQuery[i] != "AND")
            {
                cout << "SYNTAX ERROR" << endl;
                return false;
            }
            i++;
        }
    }

    return true;
}

bool semanticParsePATH()
{
    logger.log("semanticParsePATH");
    
    // Result relation must not exist
    if (tableCatalogue.isTable(parsedQuery.pathResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    // Graph must exist
    if (!graphCatalogue.isGraph(parsedQuery.pathGraphName))
    {
        cout << "SEMANTIC ERROR: Graph doesn't exist" << endl;
        return false;
    }

    // src and dest IDs must be non-negative
    if (parsedQuery.pathSrcNodeID < 0 ||
        parsedQuery.pathDestNodeID < 0)
    {
        cout << "SEMANTIC ERROR: Invalid node id" << endl;
        return false;
    }

    return true;
}


/**
 * @brief Helper to extract bit value from multi-integer attribute mask
 * @param masks Vector of 8 integers representing the attribute mask
 * @param attrName Attribute name like "A1", "A2", "B1"
 * @return 0 or 1, or -1 if attribute doesn't exist
 */
int getAttributeValue(const vector<int>& masks, const string& attrName)
{
    if (attrName.length() < 2) return -1;
    
    char type = attrName[0]; // 'A' or 'B'
    int index;
    try {
        index = stoi(attrName.substr(1)) - 1; // A1 -> bit 0, A2 -> bit 1
    } catch (...) {
        return -1;
    }
    
    if (index < 0 || index >= 250) return -1;
    
    int maskIndex = index / 32;      // Which integer (0-7)
    int bitPos = index % 32;          // Which bit in that integer (0-31)
    
    return (masks[maskIndex] >> bitPos) & 1;
}

/**
 * @brief Check if a specific bit is set in the multi-integer mask
 */
bool isBitSet(const vector<int>& masks, int bitIndex)
{
    if (bitIndex < 0 || bitIndex >= 250) return false;
    
    int maskIndex = bitIndex / 32;
    int bitPos = bitIndex % 32;
    
    return (masks[maskIndex] >> bitPos) & 1;
}

/**
 * @brief Get node masks from node table using optimized binary search
 * @return Vector of 8 integers, or empty vector if not found
 */
vector<int> getNodeMask(GraphStorage* storage, int nodeID)
{
    return storage->getNodeMaskById(nodeID);
}

/**
 * @brief Reconstruct path from parent map
 */
vector<int> reconstructPath(const unordered_map<int, int>& parent, int src, int dest)
{
    vector<int> path;
    int current = dest;
    
    while (current != src)
    {
        path.push_back(current);
        current = parent.at(current);
    }
    path.push_back(src);
    
    reverse(path.begin(), path.end());
    return path;
}

/**
 * @brief Helper to check if conditions match with specific uniform values
 * Note: ANY conditions should be expanded to specific attributes before calling this function
 */
bool conditionsMatchWithValues(const vector<int>& masks, const vector<PathCondition>& conditions, 
                                 const unordered_map<string, int>& uniformValues, char applyTo)
{
    for (const auto& cond : conditions)
    {
        if (cond.appliesTo != applyTo) continue;
        
        // Specific attribute only (ANY should be expanded already)
        int val = getAttributeValue(masks, cond.attribute);
        if (val == -1) continue;
        
        if (cond.value == -1)
        {
            // Uniform condition - use the value from uniformValues
            string key = cond.attribute + "(" + applyTo + ")";
            if (uniformValues.count(key))
            {
                if (val != uniformValues.at(key))
                    return false;
            }
        }
        else
        {
            // Specific value
            if (val != cond.value)
                return false;
        }
    }
    return true;
}

/**
 * @brief Run Dijkstra with specific uniform condition values
 */
pair<bool, int> runDijkstraWithConstraints(Graph* graph, int src, int dest,
                                            const vector<int>& srcMask, const vector<int>& destMask,
                                            const unordered_map<string, int>& uniformValues,
                                            vector<int>& outPathNodes)
{
    priority_queue<pair<int, int>, vector<pair<int, int>>, greater<pair<int, int>>> pq;
    
    unordered_map<int, int> dist;
    unordered_map<int, int> parent;
    unordered_map<int, vector<int>> nodeMasks;
    
    dist[src] = 0;
    nodeMasks[src] = srcMask;
    pq.push({0, src});
    
    bool pathFound = false;
    
    while (!pq.empty())
    {
        auto [d, u] = pq.top();
        pq.pop();
        
        if (dist.count(u) && d > dist[u])
            continue;
        
        if (u == dest)
        {
            pathFound = true;
            break;
        }
        
        vector<vector<int>> neighbors = graph->storage->getNeighbors(u);
        
        for (const auto& edge : neighbors)
        {
            int v = edge[1];
            int weight = edge[2];
            vector<int> edgeMask(edge.begin() + 3, edge.begin() + 3 + MASK_INT_COUNT);
            
            // Check edge conditions with uniform values
            if (!conditionsMatchWithValues(edgeMask, parsedQuery.pathConditions, uniformValues, 'E'))
                continue;
            
            vector<int> vMask;
            if (nodeMasks.count(v))
                vMask = nodeMasks[v];
            else
            {
                vMask = getNodeMask(graph->storage, v);
                if (vMask.empty()) continue;
                nodeMasks[v] = vMask;
            }
            
            // Check node conditions with uniform values
            if (!conditionsMatchWithValues(vMask, parsedQuery.pathConditions, uniformValues, 'N'))
                continue;
            
            int newDist = dist[u] + weight;
            if (!dist.count(v) || newDist < dist[v])
            {
                dist[v] = newDist;
                parent[v] = u;
                pq.push({newDist, v});
            }
        }
    }
    
    if (!pathFound || !dist.count(dest))
        return {false, -1};
    
    outPathNodes = reconstructPath(parent, src, dest);
    return {true, dist[dest]};
}

void executePATH()
{
    logger.log("executePATH");
    unsigned long long blockAccessesBefore = BLOCK_ACCESSES;
    int src = parsedQuery.pathSrcNodeID;
    int dest = parsedQuery.pathDestNodeID;
    
    Graph* graph = graphCatalogue.getGraph(parsedQuery.pathGraphName);
    // BLOCK_COUNT = 0;
    
    // Get source and destination node masks using optimized binary search
    vector<int> srcMask = getNodeMask(graph->storage, src);
    vector<int> destMask = getNodeMask(graph->storage, dest);
    
    if (srcMask.empty() || destMask.empty())
    {
        cout << "Node does not exist" << endl;
        return;
    }
    
    // Check if there's an ANY condition that needs expansion
    vector<PathCondition> expandedConditions;
    bool hasAnyNode = false;
    bool hasAnyEdge = false;
    int anyNodeValue = -1;  // -1 means uniform, 0/1 means specific value
    int anyEdgeValue = -1;   // -1 means uniform, 0/1 means specific value
    
    for (const auto& cond : parsedQuery.pathConditions)
    {
        if (cond.attribute == "ANY")
        {
            if (cond.appliesTo == 'N')
            {
                hasAnyNode = true;
                anyNodeValue = cond.value; // -1 for uniform, 0 or 1 for specific
            }
            else if (cond.appliesTo == 'E')
            {
                hasAnyEdge = true;
                anyEdgeValue = cond.value; // -1 for uniform, 0 or 1 for specific
            }
        }
        else
        {
            expandedConditions.push_back(cond);
        }
    }
    
    // Collect uniform conditions (excluding ANY which we'll handle separately)
    vector<PathCondition> uniformConditions;
    for (const auto& cond : expandedConditions)
    {
        if (cond.value == -1)
            uniformConditions.push_back(cond);
    }
    
    // Try all combinations of uniform condition values (0 and 1)
    int numCombinations = 1 << uniformConditions.size(); // 2^n combinations
    
    int minWeight = numeric_limits<int>::max();
    vector<int> bestPathNodes;
    bool foundValidPath = false;
    
    // If ANY condition exists, we need to try all attributes
    if (hasAnyNode || hasAnyEdge)
    {
        // Handle the combination when BOTH ANY(N) and ANY(E) are present
        if (hasAnyNode && hasAnyEdge)
        {
            // Determine which values to try for nodes
            vector<int> nodeValuesToTry;
            if (anyNodeValue == -1)
            {
                nodeValuesToTry = {0, 1};
            }
            else
            {
                nodeValuesToTry = {anyNodeValue};
            }
            
            // Determine which values to try for edges
            vector<int> edgeValuesToTry;
            if (anyEdgeValue == -1)
            {
                edgeValuesToTry = {0, 1};
            }
            else
            {
                edgeValuesToTry = {anyEdgeValue};
            }
            
            // Try all combinations of node attribute × edge attribute
            for (int nodeAttrIdx = 0; nodeAttrIdx < graph->storage->nodeAttributeCount; nodeAttrIdx++)
            {
                for (int nodeValue : nodeValuesToTry)
                {
                    for (int edgeAttrIdx = 0; edgeAttrIdx < graph->storage->edgeAttributeCount; edgeAttrIdx++)
                    {
                        for (int edgeValue : edgeValuesToTry)
                        {
                            // Create both conditions
                            PathCondition nodeCond;
                            nodeCond.attribute = "A" + to_string(nodeAttrIdx + 1);
                            nodeCond.appliesTo = 'N';
                            nodeCond.value = nodeValue;
                            
                            PathCondition edgeCond;
                            edgeCond.attribute = "B" + to_string(edgeAttrIdx + 1);
                            edgeCond.appliesTo = 'E';
                            edgeCond.value = edgeValue;
                            
                            // Add both conditions
                            vector<PathCondition> testConditions = expandedConditions;
                            testConditions.push_back(nodeCond);
                            testConditions.push_back(edgeCond);
                            
                            // Swap into parsedQuery temporarily
                            auto originalConditions = parsedQuery.pathConditions;
                            parsedQuery.pathConditions = testConditions;
                            
                            // Try all uniform combinations
                            for (int combo = 0; combo < numCombinations; combo++)
                            {
                                unordered_map<string, int> uniformValues;
                                
                                for (size_t i = 0; i < uniformConditions.size(); i++)
                                {
                                    int uniformValue = (combo >> i) & 1;
                                    string key = uniformConditions[i].attribute + "(" + uniformConditions[i].appliesTo + ")";
                                    uniformValues[key] = uniformValue;
                                }
                                
                                // Check if src and dest satisfy conditions
                                if (!conditionsMatchWithValues(srcMask, testConditions, uniformValues, 'N') ||
                                    !conditionsMatchWithValues(destMask, testConditions, uniformValues, 'N'))
                                {
                                    continue;
                                }
                                
                                // Run Dijkstra
                                vector<int> pathNodes;
                                auto [found, weight] = runDijkstraWithConstraints(graph, src, dest, srcMask, destMask, 
                                                                                   uniformValues, pathNodes);
                                
                                if (found && weight < minWeight)
                                {
                                    minWeight = weight;
                                    bestPathNodes = pathNodes;
                                    foundValidPath = true;
                                }
                            }
                            
                            // Restore original conditions
                            parsedQuery.pathConditions = originalConditions;
                        }
                    }
                }
            }
        }
        // Only ANY(N) is present
        else if (hasAnyNode)
        {
            // Determine which values to try: if anyNodeValue is -1 (uniform), try both 0 and 1
            // If anyNodeValue is 0 or 1, try only that value
            vector<int> valuesToTry;
            if (anyNodeValue == -1)
            {
                valuesToTry = {0, 1}; // Uniform case: try both values
            }
            else
            {
                valuesToTry = {anyNodeValue}; // Specific value case
            }
            
            for (int attrIdx = 0; attrIdx < graph->storage->nodeAttributeCount; attrIdx++)
            {
                for (int value : valuesToTry)
                {
                    // Create condition: Ai(N) == value
                    PathCondition specificCond;
                    specificCond.attribute = "A" + to_string(attrIdx + 1);
                    specificCond.appliesTo = 'N';
                    specificCond.value = value;
                    
                    // Temporarily add this condition
                    vector<PathCondition> testConditions = expandedConditions;
                    testConditions.push_back(specificCond);
                    
                    // Swap into parsedQuery temporarily
                    auto originalConditions = parsedQuery.pathConditions;
                    parsedQuery.pathConditions = testConditions;
                    
                    // Try all uniform combinations with this specific attribute
                    for (int combo = 0; combo < numCombinations; combo++)
                    {
                        unordered_map<string, int> uniformValues;
                        
                        for (size_t i = 0; i < uniformConditions.size(); i++)
                        {
                            int uniformValue = (combo >> i) & 1;
                            string key = uniformConditions[i].attribute + "(" + uniformConditions[i].appliesTo + ")";
                            uniformValues[key] = uniformValue;
                        }
                        
                        // Check if src and dest satisfy conditions
                        if (!conditionsMatchWithValues(srcMask, testConditions, uniformValues, 'N') ||
                            !conditionsMatchWithValues(destMask, testConditions, uniformValues, 'N'))
                        {
                            continue;
                        }
                        
                        // Run Dijkstra
                        vector<int> pathNodes;
                        auto [found, weight] = runDijkstraWithConstraints(graph, src, dest, srcMask, destMask, 
                                                                           uniformValues, pathNodes);
                        
                        if (found && weight < minWeight)
                        {
                            minWeight = weight;
                            bestPathNodes = pathNodes;
                            foundValidPath = true;
                        }
                    }
                    
                    // Restore original conditions
                    parsedQuery.pathConditions = originalConditions;
                }
            }
        }
        // Only ANY(E) is present
        else if (hasAnyEdge)
        {
            // Determine which values to try
            vector<int> valuesToTry;
            if (anyEdgeValue == -1)
            {
                valuesToTry = {0, 1}; // Uniform case: try both values
            }
            else
            {
                valuesToTry = {anyEdgeValue}; // Specific value case
            }
            
            for (int attrIdx = 0; attrIdx < graph->storage->edgeAttributeCount; attrIdx++)
            {
                for (int value : valuesToTry)
                {
                    // Create condition: Bi(E) == value
                    PathCondition specificCond;
                    specificCond.attribute = "B" + to_string(attrIdx + 1);
                    specificCond.appliesTo = 'E';
                    specificCond.value = value;
                    
                    // Temporarily add this condition
                    vector<PathCondition> testConditions = expandedConditions;
                    testConditions.push_back(specificCond);
                    
                    // Swap into parsedQuery temporarily
                    auto originalConditions = parsedQuery.pathConditions;
                    parsedQuery.pathConditions = testConditions;
                    
                    // Try all uniform combinations with this specific attribute
                    for (int combo = 0; combo < numCombinations; combo++)
                    {
                        unordered_map<string, int> uniformValues;
                        
                        for (size_t i = 0; i < uniformConditions.size(); i++)
                        {
                            int uniformValue = (combo >> i) & 1;
                            string key = uniformConditions[i].attribute + "(" + uniformConditions[i].appliesTo + ")";
                            uniformValues[key] = uniformValue;
                        }
                        
                        // Check if src and dest satisfy conditions
                        if (!conditionsMatchWithValues(srcMask, testConditions, uniformValues, 'N') ||
                            !conditionsMatchWithValues(destMask, testConditions, uniformValues, 'N'))
                        {
                            continue;
                        }
                        
                        // Run Dijkstra
                        vector<int> pathNodes;
                        auto [found, weight] = runDijkstraWithConstraints(graph, src, dest, srcMask, destMask, 
                                                                           uniformValues, pathNodes);
                        
                        if (found && weight < minWeight)
                        {
                            minWeight = weight;
                            bestPathNodes = pathNodes;
                            foundValidPath = true;
                        }
                    }
                    
                    // Restore original conditions
                    parsedQuery.pathConditions = originalConditions;
                }
            }
        }
    }
    else
    {
        // No ANY condition: just try all uniform combinations
        for (int combo = 0; combo < numCombinations; combo++)
        {
            // Build uniform values map for this combination
            unordered_map<string, int> uniformValues;
            
            for (size_t i = 0; i < uniformConditions.size(); i++)
            {
                int value = (combo >> i) & 1; // Extract i-th bit
                string key = uniformConditions[i].attribute + "(" + uniformConditions[i].appliesTo + ")";
                uniformValues[key] = value;
            }
            
            // Check if src and dest satisfy conditions with these uniform values
            if (!conditionsMatchWithValues(srcMask, parsedQuery.pathConditions, uniformValues, 'N') ||
                !conditionsMatchWithValues(destMask, parsedQuery.pathConditions, uniformValues, 'N'))
            {
                continue;
            }
            
            // Run Dijkstra with these uniform values
            vector<int> pathNodes;
            auto [found, weight] = runDijkstraWithConstraints(graph, src, dest, srcMask, destMask, 
                                                               uniformValues, pathNodes);
            
            if (found && weight < minWeight)
            {
                minWeight = weight;
                bestPathNodes = pathNodes;
                foundValidPath = true;
            }
        }
    }
    
    if (!foundValidPath)
    {
        cout << "FALSE" << endl;
        return;
    }
    
    vector<int> pathNodes = bestPathNodes;
    
    // Collect node masks for path nodes
    unordered_map<int, vector<int>> nodeMasks;
    for (int nodeID : pathNodes)
    {
        nodeMasks[nodeID] = getNodeMask(graph->storage, nodeID);
    }
    
    // Materialize the result graph
    string resultGraphName = parsedQuery.pathResultRelationName;
    string suffix = graph->directed ? "_D" : "_U";
    
    // Create node and edge CSV files in temp directory
    string nodeFile = "../data/temp/" + resultGraphName + "_Nodes" + suffix + ".csv";
    string edgeFile = "../data/temp/" + resultGraphName + "_Edges" + suffix + ".csv";
    
    ofstream nodeOut(nodeFile);
    nodeOut << "NodeID";
    
    // Get max attributes from original graph
    int maxNodeAttrs = graph->storage->nodeAttributeCount;
    
    for (int i = 1; i <= maxNodeAttrs; i++)
        nodeOut << ",A" << i;
    nodeOut << "\n";
    
    // Write nodes - FIX: ensure all bits are checked correctly
    for (int nodeID : pathNodes)
    {
        nodeOut << nodeID;
        const vector<int>& masks = nodeMasks[nodeID];
        
        // Write each attribute bit
        for (int attrIdx = 0; attrIdx < maxNodeAttrs; attrIdx++)
        {
            int maskIndex = attrIdx / 32;      // Which integer (0-7)
            int bitPos = attrIdx % 32;          // Which bit in that integer (0-31)
            int value = (masks[maskIndex] >> bitPos) & 1;
            nodeOut << "," << value;
        }
        nodeOut << "\n";
    }
    nodeOut.close();
    
    // Write edges
    ofstream edgeOut(edgeFile);
    edgeOut << "Src,Dest,Weight";
    int maxEdgeAttrs = graph->storage->edgeAttributeCount;
    
    for (int i = 1; i <= maxEdgeAttrs; i++)
        edgeOut << ",B" << i;
    edgeOut << "\n";
    
    for (size_t i = 0; i < pathNodes.size() - 1; i++)
    {
        int u = pathNodes[i];
        int v = pathNodes[i + 1];
        
        vector<vector<int>> neighbors = graph->storage->getNeighbors(u);
        for (const auto& edge : neighbors)
        {
            if (edge[1] == v)
            {
                edgeOut << u << "," << v << "," << edge[2];
                
                // Extract edge mask (columns 3 to 3+MASK_INT_COUNT)
                vector<int> edgeMask(edge.begin() + 3, edge.begin() + 3 + MASK_INT_COUNT);
                
                // Write each edge attribute bit
                for (int attrIdx = 0; attrIdx < maxEdgeAttrs; attrIdx++)
                {
                    int maskIndex = attrIdx / 32;
                    int bitPos = attrIdx % 32;
                    int value = (edgeMask[maskIndex] >> bitPos) & 1;
                    edgeOut << "," << value;
                }
                edgeOut << "\n";
                break;
            }
        }
    }
    edgeOut.close();
    unsigned long long pathBlockAccesses = BLOCK_ACCESSES - blockAccessesBefore;
    cout << "PATH block accesses: " << pathBlockAccesses << endl;
    // cout <<"Block count: " << BLOCK_COUNT << endl;
    // Load the result graph into the catalogue as a temporary graph
    Graph* resultGraph = new Graph(resultGraphName, graph->directed, true);  // Mark as temporary
    resultGraph->load(false);  // Don't print "Loaded Graph" message for PATH results
    graphCatalogue.insertGraph(resultGraph);
    // Output result
    cout << "TRUE " << minWeight << endl;
}