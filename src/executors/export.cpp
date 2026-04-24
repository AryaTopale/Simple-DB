#include "global.h"

/**
 * @brief 
 * SYNTAX: EXPORT <relation_name> 
 */

bool syntacticParseEXPORT()
{
    logger.log("syntacticParseEXPORT");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = EXPORT;
    parsedQuery.exportRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParseEXPORT()
{
    logger.log("semanticParseEXPORT");
    //Table should exist
    if (tableCatalogue.isTable(parsedQuery.exportRelationName))
        return true;
    cout << "SEMANTIC ERROR: No such relation exists" << endl;
    return false;
}

void executeEXPORT()
{
    logger.log("executeEXPORT");
    Table* table = tableCatalogue.getTable(parsedQuery.exportRelationName);
    table->makePermanent();
    return;
}

/**
 * @brief 
 * SYNTAX: EXPORT GRAPH <graph_name>
 */

bool syntacticParseEXPORT_GRAPH()
{
    logger.log("syntacticParseEXPORT_GRAPH");
    if (tokenizedQuery.size() != 3 || tokenizedQuery[1] != "GRAPH")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = EXPORT_GRAPH;
    parsedQuery.exportGraphName = tokenizedQuery[2];
    return true;
}

bool semanticParseEXPORT_GRAPH()
{
    logger.log("semanticParseEXPORT_GRAPH");
    // Graph should exist
    if (graphCatalogue.isGraph(parsedQuery.exportGraphName))
        return true;
    cout << "SEMANTIC ERROR: Graph doesn't exist" << endl;
    return false;
}

void executeEXPORT_GRAPH()
{
    logger.log("executeEXPORT_GRAPH");
    
    Graph* graph = graphCatalogue.getGraph(parsedQuery.exportGraphName);
    string suffix = graph->directed ? "_D" : "_U";
    
    // Export nodes
    string nodeFileName = "../data/" + graph->graphName + "_Nodes" + suffix + ".csv";
    string edgeFileName = "../data/" + graph->graphName + "_Edges" + suffix + ".csv";
    
    // Get the original input file names to read headers
    string originalNodeFile = graph->getNodeFileName();
    string originalEdgeFile = graph->getEdgeFileName();
    
    // Read original headers to get attribute names
    ifstream nodeHeaderFile(originalNodeFile);
    ifstream edgeHeaderFile(originalEdgeFile);
    
    string nodeHeader, edgeHeader;
    if (nodeHeaderFile.is_open())
    {
        getline(nodeHeaderFile, nodeHeader);
        nodeHeaderFile.close();
    }
    if (edgeHeaderFile.is_open())
    {
        getline(edgeHeaderFile, edgeHeader);
        edgeHeaderFile.close();
    }
    
    // Parse node attributes from header
    vector<string> nodeAttributes;
    stringstream nodeHeaderStream(nodeHeader);
    string attr;
    getline(nodeHeaderStream, attr, ','); // Skip NodeID
    while (getline(nodeHeaderStream, attr, ','))
    {
        attr.erase(remove_if(attr.begin(), attr.end(), ::isspace), attr.end());
        nodeAttributes.push_back(attr);
    }
    
    vector<string> edgeAttributes;
    stringstream edgeHeaderStream(edgeHeader);
    getline(edgeHeaderStream, attr, ','); // Skip Src_NodeID
    getline(edgeHeaderStream, attr, ','); // Skip Dest_NodeID
    getline(edgeHeaderStream, attr, ','); // Skip Weight
    while (getline(edgeHeaderStream, attr, ','))
    {
        attr.erase(remove_if(attr.begin(), attr.end(), ::isspace), attr.end());
        edgeAttributes.push_back(attr);
    }
    
    // Export nodes
    ofstream nodeOut(nodeFileName);
    nodeOut << "NodeID";
    for (const string& attr : nodeAttributes)
    {
        nodeOut << ", " << attr;
    }
    nodeOut << endl;
    
    Table* nodeTable = tableCatalogue.getTable(graph->storage->nodeTableName);
    Cursor nodeCursor(graph->storage->nodeTableName, 0);
    vector<int> nodeRow;
    
    while (!(nodeRow = nodeCursor.getNext()).empty())
    {
        int nodeID = nodeRow[0];
        nodeOut << nodeID;
        
        // Convert masks back to individual attributes
        for (int attrIdx = 0; attrIdx < (int)nodeAttributes.size(); attrIdx++)
        {
            int maskIndex = attrIdx / 32;
            int bitPos = attrIdx % 32;
            int value = (nodeRow[1 + maskIndex] & (1 << bitPos)) ? 1 : 0;
            nodeOut << "," << value;
        }
        nodeOut << endl;
    }
    nodeOut.close();
    
    // Export edges (use original edge table to preserve input order)
    ofstream edgeOut(edgeFileName);
    edgeOut << "Src_NodeID, Dest_NodeID, Weight";
    for (const string& attr : edgeAttributes)
    {
        edgeOut << ", " << attr;
    }
    edgeOut << endl;
    
    // Use cursor to read edges from original edge table
    Table* edgeTable = tableCatalogue.getTable(graph->storage->edgeOriginalTableName);
    Cursor edgeCursor(graph->storage->edgeOriginalTableName, 0);
    vector<int> edgeRow;
    
    while (!(edgeRow = edgeCursor.getNext()).empty())
    {
        int src = edgeRow[0];
        int dst = edgeRow[1];
        int weight = edgeRow[2];
        edgeOut << src << ", " << dst << ", " << weight;
        
        // Convert masks back to individual attributes
        for (int attrIdx = 0; attrIdx < (int)edgeAttributes.size(); attrIdx++)
        {
            int maskIndex = attrIdx / 32;
            int bitPos = attrIdx % 32;
            int value = (edgeRow[3 + maskIndex] & (1 << bitPos)) ? 1 : 0;
            edgeOut << "," << value;
        }
        edgeOut << endl;
    }
    edgeOut.close();
    
    cout << "Graph exported successfully" << endl;
    return;
}