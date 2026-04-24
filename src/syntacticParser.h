#pragma once
#include "tableCatalogue.h"

using namespace std;

enum QueryType
{
    CLEAR,
    CROSS,
    DISTINCT,
    EXPORT,
    INDEX,
    JOIN,
    LIST,
    LOAD,
    PRINT,
    PROJECTION,
    RENAME,
    SELECTION,
    SORT,
    SOURCE,
    
    // Graph Commands (Phase 1)
    LOAD_GRAPH,
    EXPORT_GRAPH,
    PATH,
    DEGREE,
    PRINT_GRAPH,

    SETBUFFER,
    GROUP_BY,
    TRANSACTION,

    UNDETERMINED
};

enum BinaryOperator
{
    LESS_THAN,
    GREATER_THAN,
    LEQ,
    GEQ,
    EQUAL,
    NOT_EQUAL,
    NO_BINOP_CLAUSE
};

enum SortingStrategy
{
    ASC,
    DESC,
    NO_SORT_CLAUSE
};

enum SelectType
{
    COLUMN,
    INT_LITERAL,
    NO_SELECT_CLAUSE
};

// New Enum for Graph Direction
enum GraphType
{
    DIRECTED,
    UNDIRECTED
};

// Structure to hold PATH conditions (e.g., A1(N) == 1)
struct PathCondition {
    string attribute;   // A1, B2, ANY
    char appliesTo;     // 'N' or 'E'
    int value;          // 0, 1, or -1 (means uniform)
};

enum JoinConditionType
{
    JOIN_ATTR_EQUAL,
    JOIN_ARITH_EXPR
};

enum AggregateFunction
{
    MAX_AGG,
    MIN_AGG,
    COUNT_AGG,
    SUM_AGG,
    AVG_AGG,
    NO_AGG
};

struct AggregateExpr {
    AggregateFunction func = NO_AGG;
    string columnName = "";  // "*" for COUNT(*)
};

class ParsedQuery
{

public:
    QueryType queryType = UNDETERMINED;

    string clearRelationName = "";

    string crossResultRelationName = "";
    string crossFirstRelationName = "";
    string crossSecondRelationName = "";

    string distinctResultRelationName = "";
    string distinctRelationName = "";

    string exportRelationName = "";

    IndexingStrategy indexingStrategy = NOTHING;
    string indexColumnName = "";
    string indexRelationName = "";

    BinaryOperator joinBinaryOperator = NO_BINOP_CLAUSE;
    string joinResultRelationName = "";
    string joinFirstRelationName = "";
    string joinSecondRelationName = "";
    string joinFirstColumnName = "";
    string joinSecondColumnName = "";
    JoinConditionType joinConditionType = JOIN_ATTR_EQUAL;
    string joinArithOp = "";
    int joinArithNumber = 0;
    bool joinHasWhere = false;
    string joinWhereTable = "";
    string joinWhereColumn = "";
    BinaryOperator joinWhereBinaryOperator = NO_BINOP_CLAUSE;
    int joinWhereNumber = 0;
    bool joinHasProject = false;
    vector<pair<string,string>> joinProjectList;
    string loadRelationName = "";

    string printRelationName = "";

    string projectionResultRelationName = "";
    vector<string> projectionColumnList;
    string projectionRelationName = "";

    string renameFromColumnName = "";
    string renameToColumnName = "";
    string renameRelationName = "";

    SelectType selectType = NO_SELECT_CLAUSE;
    BinaryOperator selectionBinaryOperator = NO_BINOP_CLAUSE;
    string selectionResultRelationName = "";
    string selectionRelationName = "";
    string selectionFirstColumnName = "";
    string selectionSecondColumnName = "";
    int selectionIntLiteral = 0;

    SortingStrategy sortingStrategy = NO_SORT_CLAUSE;
    string sortResultRelationName = "";
    string sortColumnName = "";
    string sortRelationName = "";

    // New multi-column sort fields
    vector<string> sortColumnNames;
    vector<SortingStrategy> sortingStrategies;
    int sortTopRows = -1;    // -1 means not specified
    int sortBottomRows = -1; // -1 means not specified

    string sourceFileName = "";

    /* --- NEW GRAPH FIELDS --- */
    string loadGraphName = "";
    GraphType graphType = DIRECTED; 

    string exportGraphName = "";
    string printGraphName = "";
    bool isPrintGraph = false; // To differentiate between PRINT and PRINT GRAPH
    
    // Fields for DEGREE command
    string degreeGraphName = "";
    int degreeNodeID = -1;

    // Fields for PATH command
    string pathResultRelationName = "";
    string pathGraphName = "";
    int pathSrcNodeID = -1;
    int pathDestNodeID = -1;
    vector<PathCondition> pathConditions; // Stores the WHERE clause conditions

    // Fields for SETBUFFER command
    int setBufferCount = 10;

    // Fields for TRANSACTION command
    string transactionFileName = "";

    // Fields for GROUP BY command
    vector<string> groupByResultRelations;
    vector<string> groupByGroupAttrs;
    string groupBySourceRelation = "";
    AggregateExpr groupByHavingLHS;
    BinaryOperator groupByHavingOp = NO_BINOP_CLAUSE;
    bool groupByHavingRHSIsAggregate = false;
    AggregateExpr groupByHavingRHS;
    int groupByHavingRHSValue = 0;
    vector<AggregateExpr> groupByReturnAggregates;

    ParsedQuery();
    void clear();
};

bool syntacticParse();
bool syntacticParseCLEAR();
bool syntacticParseCROSS();
bool syntacticParseDISTINCT();
bool syntacticParseEXPORT();
bool syntacticParseINDEX();
bool syntacticParseJOIN();
bool syntacticParseLIST();
bool syntacticParseLOAD();
bool syntacticParsePRINT();
bool syntacticParsePROJECTION();
bool syntacticParseRENAME();
bool syntacticParseSELECTION();
bool syntacticParseSORT();
bool syntacticParseSOURCE();

// New Graph Syntactic Parse Functions
bool syntacticParseLOAD_GRAPH();
bool syntacticParseEXPORT_GRAPH();
bool syntacticParsePATH();
bool syntacticParseDEGREE();
bool syntacticParsePRINT_GRAPH();

bool syntacticParseSETBUFFER();
bool syntacticParseGROUP_BY();
bool syntacticParseTRANSACTION();

bool isFileExists(string tableName);
bool isQueryFile(string fileName);