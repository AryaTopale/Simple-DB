#include"global.h"

bool semanticParse(){
    logger.log("semanticParse");
    switch(parsedQuery.queryType){
        // Table Commands
        case CLEAR: return semanticParseCLEAR();
        case CROSS: return semanticParseCROSS();
        case DISTINCT: return semanticParseDISTINCT();
        case EXPORT: return semanticParseEXPORT();
        case INDEX: return semanticParseINDEX();
        case JOIN: return semanticParseJOIN();
        case LIST: return semanticParseLIST();
        case LOAD: return semanticParseLOAD();
        case PRINT: return semanticParsePRINT();
        case PROJECTION: return semanticParsePROJECTION();
        case RENAME: return semanticParseRENAME();
        case SELECTION: return semanticParseSELECTION();
        case SORT: return semanticParseSORT();
        case SOURCE: return semanticParseSOURCE();

        // Graph Commands (Phase 1)
        case LOAD_GRAPH: return semanticParseLOAD_GRAPH();     // Section 3.1 
        case EXPORT_GRAPH: return semanticParseEXPORT_GRAPH(); // Section 3.2
        case PATH: return semanticParsePATH();
        case DEGREE: return semanticParseDEGREE();                 // Section 3.4
        case SETBUFFER: return semanticParseSETBUFFER();
        case GROUP_BY: return semanticParseGROUP_BY();
        case TRANSACTION: return semanticParseTRANSACTION();
        default: cout<<"SEMANTIC ERROR"<<endl;
    }

    return false;
}

bool semanticParseSETBUFFER()
{
    logger.log("semanticParseSETBUFFER");
    if (parsedQuery.setBufferCount < 2 || parsedQuery.setBufferCount > 10)
    {
        cout << "SEMANTIC ERROR: Buffer size K must satisfy 2 <= K <= 10" << endl;
        return false;
    }
    return true;
}

bool semanticParseTRANSACTION()
{
    logger.log("semanticParseTRANSACTION");
    // Check that the input schedule file exists in ../data/
    string filePath = "../data/" + parsedQuery.transactionFileName;
    struct stat buffer;
    if (stat(filePath.c_str(), &buffer) != 0)
    {
        cout << "SEMANTIC ERROR: Transaction schedule file doesn't exist" << endl;
        return false;
    }
    return true;
}
