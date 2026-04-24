#include "global.h"
#include "graphCatalogue.h"
/**
 * @brief 
 * SYNTAX: LOAD relation_name
 */
bool syntacticParseLOAD()
{
    logger.log("syntacticParseLOAD");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = LOAD;
    parsedQuery.loadRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParseLOAD()
{
    logger.log("semanticParseLOAD");
    if (tableCatalogue.isTable(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: Relation already exists" << endl;
        return false;
    }

    if (!isFileExists(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
        return false;
    }
    return true;
}

void executeLOAD()
{
    logger.log("executeLOAD");

    Table *table = new Table(parsedQuery.loadRelationName);
    if (table->load())
    {
        tableCatalogue.insertTable(table);
        cout << "Loaded Table. Column Count: " << table->columnCount << " Row Count: " << table->rowCount << endl;
    }
    return;
}

bool syntacticParseLOAD_GRAPH()
{
    logger.log("syntacticParseLOAD_GRAPH");
    // logger.log(tokenizedQuery.size())
    // // cout<<tokenizedQuery.size()<<endl;
    if (tokenizedQuery.size() != 4 || tokenizedQuery[1] != "GRAPH")
    {
        cout << "SYNTAX ERROR" << endl;
        // cout<<tokenizedQuery.size()<<endl;
        return false;
    }

    string type = tokenizedQuery[3];
    if (type != "U" && type != "D")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = LOAD_GRAPH;
    parsedQuery.loadGraphName = tokenizedQuery[2];
    // Assuming ParsedQuery struct has a field for graphType or we store it purely for the semantic step
    parsedQuery.graphType = (type == "D") ? DIRECTED : UNDIRECTED; 

    return true;
}

bool semanticParseLOAD_GRAPH()
{
    logger.log("semanticParseLOAD_GRAPH");
    if (graphCatalogue.isGraph(parsedQuery.loadGraphName))
    {
        cout << "SEMANTIC ERROR: Graph already exists" << endl;
        return false;
    }
    string suffix = (parsedQuery.graphType == DIRECTED) ? "_D" : "_U";
    string nodeTableName = parsedQuery.loadGraphName + "_Nodes" + suffix;
    string edgeTableName = parsedQuery.loadGraphName + "_Edges" + suffix;
    if (!isFileExists(nodeTableName))
    {
        cout << "SEMANTIC ERROR: Node file doesn't exist" << endl;
        return false;
    }
    
    if (!isFileExists(edgeTableName))
    {
        cout << "SEMANTIC ERROR: Edge file doesn't exist" << endl;
        return false;
    }

    return true;
}
void executeLOAD_GRAPH()
{
    logger.log("executeLOAD_GRAPH");
    bool isDirected = (parsedQuery.graphType == DIRECTED);
    Graph *graph = new Graph(parsedQuery.loadGraphName, isDirected);
    if (graph->load())
    {
        graphCatalogue.insertGraph(graph);
        // cout << "Loaded Graph. Node Count: " << graph->storage->nodeRowcount 
        //      << ", Edge Count: " << graph->storage->edgeRowcount << endl;
    }
    else
    {
        delete graph;
    }
    return;
}