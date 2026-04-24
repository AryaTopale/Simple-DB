#include "graphstorage.h"
#include "global.h"
#include <algorithm>
#include <fstream>
#include <sstream>

// Configuration: Number of integers needed to store 250 bits
// Each int holds 32 bits, so we need ceil(250/32) = 8 integers
const int MASK_INT_COUNT = 8; // 8 * 32 = 256 bits (covers 250 attributes)

GraphStorage::GraphStorage(const string &graphName, bool directed)
    : _graphName(graphName), _directed(directed)
{
    logger.log("GraphStorage::GraphStorage");

    nodeTableName = graphName + "_NodesGraph";
    nodeTableNameSorted = graphName + "_NodesGraphSorted";
    edgeTableName = graphName + "_EdgesGraph";
    edgeAdjTableName = graphName + "_EdgeAdjGraph";
    indexTableName = graphName + "_IndexGraph";
    edgeOriginalTableName = graphName + "_EdgesOriginalGraph";
    degreeTableName = graphName + "_DegreeGraph";

    nodeRowcount = 0;
    edgeRowcount = 0;
}

// Helper function to parse attributes into multiple mask integers
vector<int> parseAttributeMask(stringstream &ss, int maxAttributes = 250)
{
    vector<int> masks(MASK_INT_COUNT, 0);
    string word;
    int attrIndex = 0;
    
    while (getline(ss, word, ',') && attrIndex < maxAttributes)
    {
        int value = stoi(word);
        if (value == 1)
        {
            int maskIndex = attrIndex / 32;  // Which integer (0-7)
            int bitPos = attrIndex % 32;     // Which bit in that integer (0-31)
            masks[maskIndex] |= (1 << bitPos);
        }
        attrIndex++;
    }
    
    return masks;
}

bool GraphStorage::loadNodesFromCsv(const string &nodeCsvPath)
{
    logger.log("GraphStorage::loadNodesFromCsv");

    ifstream fin(nodeCsvPath);
    if (!fin.is_open())
        return false;

    // Create table with node_id + 8 mask columns
    string line, word;
    getline(fin, line);
    stringstream headerStream(line);
    getline(headerStream, word, ','); // Skip "NodeID"
    
    nodeAttributeCount = 0;
    while (getline(headerStream, word, ','))
    {
        nodeAttributeCount++;
    }

    // Create table with node_id + 8 mask columns
    vector<string> columns = {"node_id"};
    for (int i = 0; i < MASK_INT_COUNT; i++)
    {
        columns.push_back("attr_mask_" + to_string(i));
    }
    
    Table *nodeTable = new Table(nodeTableName, columns);
    tableCatalogue.insertTable(nodeTable);

    // Load all nodes into memory (preserving original order)
    vector<vector<int>> allNodeRows;
    vector<int> row(1 + MASK_INT_COUNT); // node_id + 8 masks

    while (getline(fin, line))
    {
        if (line.empty())
            continue;
        stringstream ss(line);

        if (!getline(ss, word, ','))
            continue;
        row[0] = stoi(word); // node_id

        // Parse attributes into multiple masks
        vector<int> masks = parseAttributeMask(ss);
        for (int i = 0; i < MASK_INT_COUNT; i++)
        {
            row[1 + i] = masks[i];
        }

        allNodeRows.push_back(row);
    }

    // Write original order to nodeTable (for PRINT command)
    vector<vector<int>> page(nodeTable->maxRowsPerBlock, vector<int>(1 + MASK_INT_COUNT));
    int rowCounter = 0;

    for (const auto& nodeRow : allNodeRows)
    {
        page[rowCounter++] = nodeRow;
        nodeTable->rowCount++;
        this->nodeRowcount++;

        if (rowCounter == nodeTable->maxRowsPerBlock)
        {
            bufferManager.writePage(nodeTableName, nodeTable->blockCount, page, rowCounter);
            nodeTable->rowsPerBlockCount.push_back(rowCounter);
            nodeTable->blockCount++;
            rowCounter = 0;
        }
    }

    if (rowCounter > 0)
    {
        bufferManager.writePage(nodeTableName, nodeTable->blockCount, page, rowCounter);
        nodeTable->rowsPerBlockCount.push_back(rowCounter);
        nodeTable->blockCount++;
    }

    // Create sorted table for binary search
    Table *nodeTableSorted = new Table(nodeTableNameSorted, columns);
    tableCatalogue.insertTable(nodeTableSorted);

    // Sort nodes by node_id
    sort(allNodeRows.begin(), allNodeRows.end(), 
         [](const vector<int>& a, const vector<int>& b) {
             return a[0] < b[0];
         });

    // Write sorted nodes to separate table
    vector<vector<int>> sortedPage(nodeTableSorted->maxRowsPerBlock, vector<int>(1 + MASK_INT_COUNT));
    int sortedRowCounter = 0;

    for (const auto& nodeRow : allNodeRows)
    {
        sortedPage[sortedRowCounter++] = nodeRow;
        nodeTableSorted->rowCount++;

        if (sortedRowCounter == nodeTableSorted->maxRowsPerBlock)
        {
            bufferManager.writePage(nodeTableNameSorted, nodeTableSorted->blockCount, sortedPage, sortedRowCounter);
            nodeTableSorted->rowsPerBlockCount.push_back(sortedRowCounter);
            nodeTableSorted->blockCount++;
            sortedRowCounter = 0;
        }
    }

    if (sortedRowCounter > 0)
    {
        bufferManager.writePage(nodeTableNameSorted, nodeTableSorted->blockCount, sortedPage, sortedRowCounter);
        nodeTableSorted->rowsPerBlockCount.push_back(sortedRowCounter);
        nodeTableSorted->blockCount++;
    }
    fin.close();
    return true;
}

/**
 * Three-pass edge loading - satisfies 2-block memory constraint
 * Pass 1: Count edges per source
 * Pass 2: Calculate cumulative positions and build index
 * Pass 3: Stream edges to disk in sorted order
 */
bool GraphStorage::loadEdgesAndBuildIndex(const string &edgeCsvPath)
{
    logger.log("GraphStorage::loadEdgesAndBuildIndex");

    // ========== PASS 0: Store edges in original order ==========
    vector<string> edgeColumns = {"src", "dst", "w"};
    for (int i = 0; i < MASK_INT_COUNT; i++)
    {
        edgeColumns.push_back("attr_mask_" + to_string(i));
    }
    
    Table *edgeOriginalTable = new Table(edgeOriginalTableName, edgeColumns);
    tableCatalogue.insertTable(edgeOriginalTable);

    ifstream fin0(edgeCsvPath);
    if (!fin0.is_open())
        return false;

    string line, word;

    // Read header and count edge attributes
    getline(fin0, line);
    stringstream headerStream(line);
    getline(headerStream, word, ','); // Skip "Src_NodeID"
    getline(headerStream, word, ','); // Skip "Dest_NodeID"
    getline(headerStream, word, ','); // Skip "Weight"

    edgeAttributeCount = 0;
    while (getline(headerStream, word, ','))
    {
        edgeAttributeCount++;
    }

    // Store edges in original order
    vector<int> edgeRow(3 + MASK_INT_COUNT); // src, dst, w, + 8 masks
    vector<vector<int>> edgePage(edgeOriginalTable->maxRowsPerBlock, edgeRow);
    int edgeRowCounter = 0;

    while (getline(fin0, line))
    {
        if (line.empty())
            continue;

        stringstream ss(line);
        getline(ss, word, ',');
        int src = stoi(word);
        getline(ss, word, ',');
        int dst = stoi(word);
        getline(ss, word, ',');
        int weight = stoi(word);

        // Parse attributes into multiple masks
        vector<int> masks = parseAttributeMask(ss, edgeAttributeCount);
        
        edgeRow[0] = src;
        edgeRow[1] = dst;
        edgeRow[2] = weight;
        for (int i = 0; i < MASK_INT_COUNT; i++)
        {
            edgeRow[3 + i] = masks[i];
        }

        edgePage[edgeRowCounter++] = edgeRow;
        edgeOriginalTable->rowCount++;
        this->originalEdgeCount++;

        if (edgeRowCounter == edgeOriginalTable->maxRowsPerBlock)
        {
            bufferManager.writePage(edgeOriginalTableName,
                                    edgeOriginalTable->blockCount,
                                    edgePage,
                                    edgeRowCounter);
            edgeOriginalTable->rowsPerBlockCount.push_back(edgeRowCounter);
            edgeOriginalTable->blockCount++;
            edgeRowCounter = 0;
        }
    }

    // Flush remaining page
    if (edgeRowCounter > 0)
    {
        bufferManager.writePage(edgeOriginalTableName,
                                edgeOriginalTable->blockCount,
                                edgePage,
                                edgeRowCounter);
        edgeOriginalTable->rowsPerBlockCount.push_back(edgeRowCounter);
        edgeOriginalTable->blockCount++;
    }
    fin0.close();

    // ========== PASS 1: Count edges per source ==========
    map<int, int> edgeCountPerSrc;
    map<int,int> edgeCountPerDest;

    ifstream fin1(edgeCsvPath);
    if (!fin1.is_open())
        return false;

    line.clear();
    word.clear();
    getline(fin1, line); // skip header

    while (getline(fin1, line))
    {
        if (line.empty())
            continue;

        stringstream ss(line);
        getline(ss, word, ',');
        int src = stoi(word);
        getline(ss, word, ',');
        int dst = stoi(word);

        edgeCountPerSrc[src]++;

        if (!_directed && src != dst)
        {
            edgeCountPerSrc[dst]++;
        }
        else if (_directed)
        {
            // For directed graphs, count incoming edges
            edgeCountPerDest[dst]++;
        }
    }
    fin1.close();

    // ========== Build Degree Table ==========
    // Create a table: node_id, in_degree, out_degree, total_degree
    Table *degreeTable = new Table(degreeTableName,
                                   {"node_id", "in_degree", "out_degree", "total_degree"});
    tableCatalogue.insertTable(degreeTable);

    // Collect all unique node IDs from node table
    set<int> allNodes;
    Cursor nodeCursor(nodeTableName, 0);
    vector<int> nodeRow;
    while (!(nodeRow = nodeCursor.getNext()).empty())
    {
        allNodes.insert(nodeRow[0]);
    }

    // Populate degree table
    vector<int> degreeRow(4);
    vector<vector<int>> degreePage(degreeTable->maxRowsPerBlock, degreeRow);
    int degreeRowCounter = 0;

    for (int nodeID : allNodes)
    {
        int outDegree = edgeCountPerSrc.count(nodeID) ? edgeCountPerSrc[nodeID] : 0;
        int inDegree = 0;

        if (!_directed)
        {
            // For undirected graphs, in-degree = out-degree
            inDegree = outDegree;
        }
        else
        {
            // For directed graphs, look up incoming edge count
            inDegree = edgeCountPerDest.count(nodeID) ? edgeCountPerDest[nodeID] : 0;
        }

        int totalDegree = inDegree + outDegree;

        degreeRow = {nodeID, inDegree, outDegree, totalDegree};
        degreePage[degreeRowCounter++] = degreeRow;
        degreeTable->rowCount++;

        if (degreeRowCounter == degreeTable->maxRowsPerBlock)
        {
            bufferManager.writePage(degreeTableName,
                                    degreeTable->blockCount,
                                    degreePage,
                                    degreeRowCounter);
            degreeTable->rowsPerBlockCount.push_back(degreeRowCounter);
            degreeTable->blockCount++;
            degreeRowCounter = 0;
        }
    }

    // Flush remaining degree page
    if (degreeRowCounter > 0)
    {
        bufferManager.writePage(degreeTableName,
                                degreeTable->blockCount,
                                degreePage,
                                degreeRowCounter);
        degreeTable->rowsPerBlockCount.push_back(degreeRowCounter);
        degreeTable->blockCount++;
    }

    // ========== PASS 2: Build index with positions ==========
    Table *edgeTable = new Table(edgeTableName, edgeColumns);
    tableCatalogue.insertTable(edgeTable);

    Table *indexTable = new Table(indexTableName,
                                  {"src_id", "start_blk", "start_off", "cnt"});
    tableCatalogue.insertTable(indexTable);

    vector<int> indexRow(4);
    vector<vector<int>> indexPage(indexTable->maxRowsPerBlock, indexRow);
    int indexRowCounter = 0;

    // Calculate where each source's edges will be written
    int currentBlock = 0;
    int currentOffset = 0;

    for (const auto &srcEntry : edgeCountPerSrc)
    {
        int srcNode = srcEntry.first;
        int count = srcEntry.second;

        // Record index entry
        indexRow = {srcNode, currentBlock, currentOffset, count};
        indexPage[indexRowCounter++] = indexRow;
        indexTable->rowCount++;

        if (indexRowCounter == indexTable->maxRowsPerBlock)
        {
            bufferManager.writePage(indexTableName,
                                    indexTable->blockCount,
                                    indexPage,
                                    indexRowCounter);
            indexTable->rowsPerBlockCount.push_back(indexRowCounter);
            indexTable->blockCount++;
            indexRowCounter = 0;
        }

        // Calculate where NEXT source will start
        currentOffset += count;
        while (currentOffset >= edgeTable->maxRowsPerBlock)
        {
            currentOffset -= edgeTable->maxRowsPerBlock;
            currentBlock++;
        }
    }

    // Flush remaining index page
    if (indexRowCounter > 0)
    {
        bufferManager.writePage(indexTableName,
                                indexTable->blockCount,
                                indexPage,
                                indexRowCounter);
        indexTable->rowsPerBlockCount.push_back(indexRowCounter);
        indexTable->blockCount++;
    }

    // ========== PASS 3: Stream edges to disk ==========
    edgeRow.clear();
    edgeRow.resize(3 + MASK_INT_COUNT);
    edgePage.clear();
    edgePage.resize(edgeTable->maxRowsPerBlock, edgeRow);

    edgeRowCounter = 0;

    // Process each source in sorted order
    for (const auto &srcEntry : edgeCountPerSrc)
    {
        int currentSrc = srcEntry.first;


        ifstream fin3(edgeCsvPath);
        getline(fin3, line); // skip header

        while (getline(fin3, line))
        {
            if (line.empty())
                continue;

            stringstream ss(line);
            getline(ss, word, ',');
            int src = stoi(word);
            getline(ss, word, ',');
            int dst = stoi(word);
            getline(ss, word, ',');
            int weight = stoi(word);

            vector<int> masks = parseAttributeMask(ss, edgeAttributeCount);

            // Write forward edge if matches current source
            if (src == currentSrc)
            {
                edgeRow[0] = src;
                edgeRow[1] = dst;
                edgeRow[2] = weight;
                for (int i = 0; i < MASK_INT_COUNT; i++)
                {
                    edgeRow[3 + i] = masks[i];
                }

                edgePage[edgeRowCounter++] = edgeRow;
                edgeTable->rowCount++;
                this->edgeRowcount++;

                if (edgeRowCounter == edgeTable->maxRowsPerBlock)
                {
                    bufferManager.writePage(edgeTableName,
                                            edgeTable->blockCount,
                                            edgePage,
                                            edgeRowCounter);
                    edgeTable->rowsPerBlockCount.push_back(edgeRowCounter);
                    edgeTable->blockCount++;
                    edgeRowCounter = 0;
                }
            }

            // Write reverse edge for undirected graph
            if (!_directed && dst == currentSrc && src != dst)
            {
                edgeRow[0] = dst;
                edgeRow[1] = src;
                edgeRow[2] = weight;
                for (int i = 0; i < MASK_INT_COUNT; i++)
                {
                    edgeRow[3 + i] = masks[i];
                }

                edgePage[edgeRowCounter++] = edgeRow;
                edgeTable->rowCount++;
                this->edgeRowcount++;

                if (edgeRowCounter == edgeTable->maxRowsPerBlock)
                {
                    bufferManager.writePage(edgeTableName,
                                            edgeTable->blockCount,
                                            edgePage,
                                            edgeRowCounter);
                    edgeTable->rowsPerBlockCount.push_back(edgeRowCounter);
                    edgeTable->blockCount++;
                    edgeRowCounter = 0;
                }
            }
        }
        fin3.close();
    }

    // Flush remaining edge page
    if (edgeRowCounter > 0)
    {
        bufferManager.writePage(edgeTableName,
                                edgeTable->blockCount,
                                edgePage,
                                edgeRowCounter);
        edgeTable->rowsPerBlockCount.push_back(edgeRowCounter);
        edgeTable->blockCount++;
    }

    return true;
}

/**
 * Retrieves all neighbors of a source node using binary search on the index.
 * Objective: minimize block accesses using O(log B) binary search
 * 
 * Index table structure: {src_id, start_blk, start_off, cnt}
 * Index table is sorted by src_id
 */
vector<vector<int>> GraphStorage::getNeighbors(int srcNodeID)
{
    vector<vector<int>> results;
    
    Table* indexTable = tableCatalogue.getTable(indexTableName);
    if (!indexTable || indexTable->blockCount == 0)
        return results;

    // Binary search on blocks using only first row of each block
    int left = 0;
    int right = indexTable->blockCount - 1;
    int targetBlock = -1;

    while (left <= right)
    {
        int mid = left + (right - left) / 2;
        
        // Read only the first row of the mid block
        Page midPage = bufferManager.getPage(indexTableName, mid);
        vector<int> firstRow = midPage.getRow(0);
        
        if (firstRow[0] == srcNodeID)
        {
            targetBlock = mid;
            break;
        }
        else if (firstRow[0] < srcNodeID)
        {
            // Target might be in this block or to the right
            // Check if srcNodeID could be in this block
            int lastRowIndex = indexTable->rowsPerBlockCount[mid] - 1;
            vector<int> lastRow = midPage.getRow(lastRowIndex);
            
            if (lastRow[0] >= srcNodeID)
            {
                targetBlock = mid;
                break;
            }
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }

    if (targetBlock == -1)
        return results; // Source node not found in index

    // Linear scan only within the target block
    Page targetPage = bufferManager.getPage(indexTableName, targetBlock);
    int rowsInBlock = indexTable->rowsPerBlockCount[targetBlock];
    
    for (int i = 0; i < rowsInBlock; i++)
    {
        vector<int> entry = targetPage.getRow(i);
        if (entry[0] == srcNodeID)
        {
            int startBlock = entry[1];
            int startOffset = entry[2];
            int edgeCount = entry[3];

            // Jump to the specific edge block
            Cursor edgeCursor(edgeTableName, startBlock);
            edgeCursor.pagePointer = startOffset;

            // Retrieve ALL edges for this source node
            for (int j = 0; j < edgeCount; j++)
            {
                vector<int> edge = edgeCursor.getNext();
                if (!edge.empty())
                {
                    results.push_back(edge);
                }
                else
                {
                    break;
                }
            }
            break;
        }
    }
    
    return results;
}

/**
 * @brief Get node attribute masks using binary search on node table
 * @param nodeID The node to query
 * @return Vector of 8 mask integers, empty if node not found
 * 
 * Optimization: Binary search on blocks using sorted node table
 * Block accesses: O(log B) where B = number of node blocks
 */
vector<int> GraphStorage::getNodeMaskById(int nodeID)
{
    Table* nodeTable = tableCatalogue.getTable(nodeTableNameSorted);
    if (!nodeTable || nodeTable->blockCount == 0)
        return {};

    // Binary search on blocks using only first row comparisons
    int left = 0;
    int right = nodeTable->blockCount - 1;
    int targetBlock = -1;

    while (left <= right)
    {
        int mid = left + (right - left) / 2;
        
        // Read only the first row of the mid block
        Page midPage = bufferManager.getPage(nodeTableNameSorted, mid);
        vector<int> firstRow = midPage.getRow(0);
        
        if (firstRow[0] == nodeID)
        {
            targetBlock = mid;
            break;
        }
        else if (firstRow[0] < nodeID)
        {
            // Check if nodeID could be in this block
            int lastRowIndex = nodeTable->rowsPerBlockCount[mid] - 1;
            vector<int> lastRow = midPage.getRow(lastRowIndex);
            
            if (lastRow[0] >= nodeID)
            {
                targetBlock = mid;
                break;
            }
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }

    if (targetBlock == -1)
        return {};

    // Linear scan only within the target block
    Page targetPage = bufferManager.getPage(nodeTableNameSorted, targetBlock);
    int rowsInBlock = nodeTable->rowsPerBlockCount[targetBlock];
    
    for (int i = 0; i < rowsInBlock; i++)
    {
        vector<int> row = targetPage.getRow(i);
        if (row[0] == nodeID)
        {
            // Return masks (columns 1-8)
            return vector<int>(row.begin() + 1, row.begin() + 1 + 8);
        }
    }
    
    return {};
}

/**
 * @brief Get degree information for a node from the degree table using binary search
 * @param nodeID The node to query
 * @return Vector with [in_degree, out_degree, total_degree], empty if not found
 * 
 * Optimization: Binary search on blocks using only first row of each block
 * Block accesses: O(log B) where B = number of blocks (~4-5 for typical graphs)
 */
vector<int> GraphStorage::getDegreeInfo(int nodeID)
{
    Table* degreeTable = tableCatalogue.getTable(degreeTableName);
    
    if (degreeTable->blockCount == 0)
        return {};
    
    // Binary search to find the rightmost block where first_node_id <= nodeID
    int left = 0;
    int right = degreeTable->blockCount - 1;
    int candidateBlock = -1;
    
    while (left <= right)
    {
        int mid = left + (right - left) / 2;
        
        // Read ONLY the first row of this block
        Cursor cursor(degreeTableName, mid);
        vector<int> firstRow = cursor.getNext();
        
        if (firstRow.empty())
        {
            right = mid - 1;
            continue;
        }
        
        int firstNodeID = firstRow[0];
        
        if (firstNodeID == nodeID)
        {
            // Found it immediately!
            return {firstRow[1], firstRow[2], firstRow[3]};
        }
        else if (firstNodeID < nodeID)
        {
            // This block's first node is less than target
            // Target could be in this block or a later block
            candidateBlock = mid;
            left = mid + 1;
        }
        else
        {
            // This block's first node is greater than target
            // Target must be in an earlier block
            right = mid - 1;
        }
    }
    
    // Now scan only the candidate block
    if (candidateBlock != -1)
    {
        Cursor finalCursor(degreeTableName, candidateBlock);
        vector<int> row;
        
        while (!(row = finalCursor.getNext()).empty())
        {
            if (row[0] == nodeID)
            {
                return {row[1], row[2], row[3]};
            }
            else if (row[0] > nodeID)
            {
                break;
            }
        }
    }
    
    return {};
}