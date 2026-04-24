#include "global.h"

bool syntacticParse()
{
    logger.log("syntacticParse");
    string possibleQueryType = tokenizedQuery[0];

    if (tokenizedQuery.size() < 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    if (possibleQueryType == "TRANSACTION")
        return syntacticParseTRANSACTION();
    else if (possibleQueryType == "CLEAR")
        return syntacticParseCLEAR();
    else if (possibleQueryType == "INDEX")
        return syntacticParseINDEX();
    else if (possibleQueryType == "LIST")
        return syntacticParseLIST();
    else if (possibleQueryType == "SETBUFFER")
        return syntacticParseSETBUFFER();
    else if (possibleQueryType == "LOAD")
    {
        if (tokenizedQuery.size() > 1 && tokenizedQuery[1] == "GRAPH")
        {
            return syntacticParseLOAD_GRAPH();
        }
        else
        {
            return syntacticParseLOAD();
        }
    }
    else if (possibleQueryType == "PRINT")
        return syntacticParsePRINT();
    else if (possibleQueryType == "RENAME")
        return syntacticParseRENAME();
    else if(possibleQueryType == "EXPORT")
    {
        if (tokenizedQuery.size() > 1 && tokenizedQuery[1] == "GRAPH")
        {
            return syntacticParseEXPORT_GRAPH();
        }
        else
        {
            return syntacticParseEXPORT();
        }
    }
    else if(possibleQueryType == "SOURCE")
        return syntacticParseSOURCE();
    else if(possibleQueryType == "DEGREE")
        return syntacticParseDEGREE();
    else if(possibleQueryType == "SORT")
        return syntacticParseSORT();
    else if(tokenizedQuery[1] == "<-" && tokenizedQuery[2] == "PATH")
        return syntacticParsePATH();
    // else if(possibleQueryType == "LOAD_GRAPH")
    //     return syntacticParseLOAD_GRAPH();
    else
    {
        // Detect GROUP BY: scan for "<-" followed by "GROUP"
        for (uint i = 0; i + 1 < tokenizedQuery.size(); i++)
        {
            if (tokenizedQuery[i] == "<-" && tokenizedQuery[i + 1] == "GROUP")
                return syntacticParseGROUP_BY();
        }

        string resultantRelationName = possibleQueryType;
        if (tokenizedQuery[1] != "<-" || tokenizedQuery.size() < 3)
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        possibleQueryType = tokenizedQuery[2];
        if (possibleQueryType == "PROJECT")
            return syntacticParsePROJECTION();
        else if (possibleQueryType == "SELECT")
            return syntacticParseSELECTION();
        else if (possibleQueryType == "JOIN")
            return syntacticParseJOIN();
        else if (possibleQueryType == "CROSS")
            return syntacticParseCROSS();
        else if (possibleQueryType == "DISTINCT")
            return syntacticParseDISTINCT();
        else
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }
    return false;
}

ParsedQuery::ParsedQuery()
{
}

void ParsedQuery::clear()
{
    logger.log("ParseQuery::clear");
    this->queryType = UNDETERMINED;

    this->clearRelationName = "";

    this->crossResultRelationName = "";
    this->crossFirstRelationName = "";
    this->crossSecondRelationName = "";

    this->distinctResultRelationName = "";
    this->distinctRelationName = "";

    this->exportRelationName = "";

    this->indexingStrategy = NOTHING;
    this->indexColumnName = "";
    this->indexRelationName = "";

    this->joinBinaryOperator = NO_BINOP_CLAUSE;
    this->joinResultRelationName = "";
    this->joinFirstRelationName = "";
    this->joinSecondRelationName = "";
    this->joinFirstColumnName = "";
    this->joinSecondColumnName = "";
    this->joinConditionType         = JOIN_ATTR_EQUAL;
    this->joinArithOp               = "";
    this->joinArithNumber           = 0;
    this->joinHasWhere              = false;
    this->joinWhereTable            = "";
    this->joinWhereColumn           = "";
    this->joinWhereBinaryOperator   = NO_BINOP_CLAUSE;
    this->joinWhereNumber           = 0;
    this->joinHasProject            = false;
    this->joinProjectList.clear();
    this->loadRelationName = "";

    this->printRelationName = "";

    this->projectionResultRelationName = "";
    this->projectionColumnList.clear();
    this->projectionRelationName = "";

    this->renameFromColumnName = "";
    this->renameToColumnName = "";
    this->renameRelationName = "";

    this->selectType = NO_SELECT_CLAUSE;
    this->selectionBinaryOperator = NO_BINOP_CLAUSE;
    this->selectionResultRelationName = "";
    this->selectionRelationName = "";
    this->selectionFirstColumnName = "";
    this->selectionSecondColumnName = "";
    this->selectionIntLiteral = 0;

    this->sortingStrategy = NO_SORT_CLAUSE;
    this->sortResultRelationName = "";
    this->sortColumnName = "";
    this->sortRelationName = "";
    this->sortColumnNames.clear();
    this->sortingStrategies.clear();
    this->sortTopRows = -1;
    this->sortBottomRows = -1;

    this->sourceFileName = "";

    // GROUP BY fields
    this->groupByResultRelations.clear();
    this->groupByGroupAttrs.clear();
    this->groupBySourceRelation = "";
    this->groupByHavingLHS = {NO_AGG, ""};
    this->groupByHavingOp = NO_BINOP_CLAUSE;
    this->groupByHavingRHSIsAggregate = false;
    this->groupByHavingRHS = {NO_AGG, ""};
    this->groupByHavingRHSValue = 0;
    this->groupByReturnAggregates.clear();

    this->transactionFileName = "";
}

/**
 * @brief Checks to see if source file exists. Called when LOAD command is
 * invoked.
 *
 * @param tableName 
 * @return true 
 * @return false 
 */
bool isFileExists(string tableName)
{
    string fileName = "../data/" + tableName + ".csv";
    struct stat buffer;
    return (stat(fileName.c_str(), &buffer) == 0);
}

/**
 * @brief Checks to see if source file exists. Called when SOURCE command is
 * invoked.
 *
 * @param tableName 
 * @return true 
 * @return false 
 */
bool isQueryFile(string fileName){
    fileName = "../data/" + fileName + ".ra";
    struct stat buffer;
    return (stat(fileName.c_str(), &buffer) == 0);
}

/**
 * @brief Parses the SETBUFFER command.
 * Syntax: SETBUFFER K
 * where 2 <= K <= 10.
 */
bool syntacticParseSETBUFFER()
{
    logger.log("syntacticParseSETBUFFER");
    // tokenizedQuery[0] == "SETBUFFER", tokenizedQuery[1] == K
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    // Ensure K is a valid integer
    for (char c : tokenizedQuery[1])
    {
        if (!isdigit(c))
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }
    parsedQuery.queryType = SETBUFFER;
    parsedQuery.setBufferCount = stoi(tokenizedQuery[1]);
    return true;
}

bool syntacticParseTRANSACTION()
{
    logger.log("syntacticParseTRANSACTION");
    // Syntax: TRANSACTION <filename>
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = TRANSACTION;
    parsedQuery.transactionFileName = tokenizedQuery[1];
    return true;
}
