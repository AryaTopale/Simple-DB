#pragma once
#include "table.h"
// #include "global.h"
#include "graphstorage.h"
typedef unsigned int uint;
class Graph
{
public:
    string graphName;
    bool directed;
    bool isTemporary;  // Flag to indicate if graph is temporary (in data/temp)

    Table* nodeTable;
    Table* edgeTable;

    GraphStorage* storage;

    Graph(string graphName, bool directed, bool isTemporary = false);
    bool load(bool printMessage = true);
    void print();

    string getNodeFileName();
    string getEdgeFileName();
    
    ~Graph();
};