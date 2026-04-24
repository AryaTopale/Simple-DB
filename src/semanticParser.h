#pragma once  // <--- CRITICAL FIX: Stops redefinition errors
#include "syntacticParser.h"

bool semanticParse();

// Table Semantic Parse Functions
bool semanticParseCLEAR();
bool semanticParseCROSS();
bool semanticParseDISTINCT();
bool semanticParseEXPORT();
bool semanticParseINDEX();
bool semanticParseJOIN();
bool semanticParseLIST();
bool semanticParseLOAD();
bool semanticParsePRINT();
bool semanticParsePROJECTION();
bool semanticParseRENAME();
bool semanticParseSELECTION();
bool semanticParseSORT();
bool semanticParseSOURCE();
bool semanticParsePATH();
bool semanticParseDEGREE();
// Graph Semantic Parse Functions (Phase 1)
bool semanticParseLOAD_GRAPH();
bool semanticParseEXPORT_GRAPH();
bool semanticParseSETBUFFER();
bool semanticParseGROUP_BY();
bool semanticParseTRANSACTION();
