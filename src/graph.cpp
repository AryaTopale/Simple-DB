#include "graph.h"
#include "global.h"

string Graph::getNodeFileName()
{
    // Suffix based on direction: _D for Directed, _U for Undirected
    string suffix = this->directed ? "_Nodes_D.csv" : "_Nodes_U.csv";
    string directory = this->isTemporary ? "../data/temp/" : "../data/";
    return directory + this->graphName + suffix;
}
string Graph::getEdgeFileName()
{
    // Suffix based on direction: _D for Directed, _U for Undirected
    string suffix = this->directed ? "_Edges_D.csv" : "_Edges_U.csv";
    string directory = this->isTemporary ? "../data/temp/" : "../data/";
    return directory + this->graphName + suffix;
}
Graph::Graph(string graphName, bool directed, bool isTemporary)
{
    this->graphName = graphName;
    this->directed = directed;
    this->isTemporary = isTemporary;
    string suffix = directed ? "_D" : "_U";
    string nodeTableName = graphName + "_Nodes" + suffix;
    string edgeTableName = graphName + "_Edges" + suffix;
    string graphStorageName = graphName + suffix;
    this->storage = new GraphStorage(graphName, directed);
    // this->nodeTable = new Table(nodeTableName);
    // this->edgeTable = new Table(edgeTableName);
    this->nodeTable = nullptr;
    this->edgeTable = nullptr;
}

bool Graph::load(bool printMessage)
{
    
    // if (this->nodeTable->load() && this->edgeTable->load())
    // {
    //     return true;
    // }
    // if (this->storage->loadNodesFromCsv(this->getNodeFileName()) &&
    //     this->storage->loadEdgesFromCsv(this->getEdgeFileName()))
    // {
    //     this->storage->buildIndexOn("Edges", 0);
    //     return true;
    // }

    if (this->storage->loadNodesFromCsv(this->getNodeFileName()) &&
    this->storage->loadEdgesAndBuildIndex(this->getEdgeFileName()))
{
    // The assignment requires specific output format: 
    // Loaded Graph. Node Count: <node_count>, Edge Count: <edge_count>
    if (printMessage)
    {
        cout << "Loaded Graph. Node Count: " << this->storage->nodeRowcount 
             << ", Edge Count: " << (this->directed ? this->storage->edgeRowcount : this->storage->edgeRowcount/2) << endl;
    }
    
    return true;
}
    
    string nodeTableName = parsedQuery.loadGraphName + "_NodesGraph";
    string edgeTableName = parsedQuery.loadGraphName + "_EdgesOriginalGraph";
    this->nodeTable = tableCatalogue.getTable(nodeTableName);
    this->edgeTable = tableCatalogue.getTable(edgeTableName);

    return false;
}

void Graph::print()
{


    cout << endl; 
    // cout << "Nodes:" << endl;
    nodeTable = tableCatalogue.getTable(this->storage->nodeTableName);
    edgeTable = tableCatalogue.getTable(this->storage->edgeOriginalTableName);
    cout << nodeTable->rowCount << endl;
    cout << edgeTable->rowCount << endl;
    cout << (this->directed ? "D" : "U") << endl<< endl;
    nodeTable->print();
    cout << endl; 
    // cout << "Edges:" << endl;
    edgeTable->print();
}

Graph::~Graph()
{
    logger.log("Graph::~Graph");
    
    // Clean up GraphStorage (which manages the actual table data)
    if (this->storage)
    {
        // Remove tables from catalogue
        if (tableCatalogue.isTable(this->storage->nodeTableName))
            tableCatalogue.deleteTable(this->storage->nodeTableName);
        if (tableCatalogue.isTable(this->storage->edgeTableName))
            tableCatalogue.deleteTable(this->storage->edgeTableName);
        if (tableCatalogue.isTable(this->storage->indexTableName))
            tableCatalogue.deleteTable(this->storage->indexTableName);
        if (tableCatalogue.isTable(this->storage->degreeTableName))
            tableCatalogue.deleteTable(this->storage->degreeTableName);
        if (tableCatalogue.isTable(this->storage->edgeAdjTableName))
            tableCatalogue.deleteTable(this->storage->edgeAdjTableName);
        if (tableCatalogue.isTable(this->storage->edgeOriginalTableName))
            tableCatalogue.deleteTable(this->storage->edgeOriginalTableName);
        
        delete this->storage;
    }
    
    // Clean up temporary CSV files for temporary graphs (created by PATH command)
    if (this->isTemporary)
    {
        string nodeFile = this->getNodeFileName();
        string edgeFile = this->getEdgeFileName();
        
        if (remove(nodeFile.c_str()) != 0)
        {
            // File might not exist, which is okay
        }
        if (remove(edgeFile.c_str()) != 0)
        {
            // File might not exist, which is okay
        }
    }
    
    // Note: Don't delete nodeTable/edgeTable directly as they either:
    // 1. Were already deleted via tableCatalogue.deleteTable above, or
    // 2. Were never properly initialized and are dangling pointers
}