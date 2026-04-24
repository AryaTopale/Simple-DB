#pragma once
#include "executor.h"
#include "tableCatalogue.h"
#include "graphCatalogue.h"
#include "syntacticParser.h"

using namespace std;
typedef unsigned int uint; 

extern float BLOCK_SIZE;
extern uint BLOCK_COUNT;
extern uint PRINT_COUNT;
extern uint MAX_BLOCKS_IN_MEMORY;
extern vector<string> tokenizedQuery;
extern ParsedQuery parsedQuery;
extern TableCatalogue tableCatalogue;
extern unsigned long long BLOCK_ACCESSES;
class GraphCatalogue; 
extern GraphCatalogue graphCatalogue;

extern BufferManager bufferManager;