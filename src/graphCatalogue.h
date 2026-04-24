#pragma once
#include "graph.h"
#include <unordered_map>
typedef unsigned int uint;
class GraphCatalogue
{
    unordered_map<string, Graph*> graphs;

public:
    GraphCatalogue() {}
    
    void insertGraph(Graph* graph);
    void deleteGraph(string graphName);
    Graph* getGraph(string graphName);
    bool isGraph(string graphName);
    void print(); 
    
    ~GraphCatalogue();
};