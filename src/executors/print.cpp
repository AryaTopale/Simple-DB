#include "global.h"
/**
 * @brief 
 * SYNTAX: PRINT relation_name
 */
bool syntacticParsePRINT()
{
    logger.log("syntacticParsePRINT");
    if (tokenizedQuery.size() == 2)
    {
        
        parsedQuery.queryType = PRINT;
        parsedQuery.printRelationName = tokenizedQuery[1];
        parsedQuery.isPrintGraph = false;
        return true;
    }
    else if (tokenizedQuery.size() == 3 && tokenizedQuery[1] == "GRAPH")
    {
        parsedQuery.queryType = PRINT;
        parsedQuery.printGraphName = tokenizedQuery[2];
        parsedQuery.isPrintGraph = true;
        return true;
    }
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    
}

bool semanticParsePRINT()
{
    logger.log("semanticParsePRINT");
    if(parsedQuery.isPrintGraph)
    {
        if (!graphCatalogue.isGraph(parsedQuery.printGraphName))
        {
            cout << "SEMANTIC ERROR: Graph doesn't exist" << endl;
            return false;
        }
    }
    else
    {
        if (!tableCatalogue.isTable(parsedQuery.printRelationName))
        {
            cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
            return false;
        }
    }
    return true;
}

void executePRINT()
{
    logger.log("executePRINT");
    if (parsedQuery.isPrintGraph)
    {
        Graph* graph = graphCatalogue.getGraph(parsedQuery.printGraphName);
        graph->print();
        return;
    }
    else{
    
        Table* table = tableCatalogue.getTable(parsedQuery.printRelationName);
        table->print();
        return;
    
    
    }

}