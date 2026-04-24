#include "../global.h"

/**
 * SYNTAX:
 * Result <- JOIN table1, table2 ON table1.col == table2.col [WHERE table.col op number] [PROJECT table.col, ...]
 */

// ─────────────────────────────────────────────────────────────────────────────
// A list of small primes used as successive hash moduli for recursive
// re-partitioning. Each level uses a different prime so rows that collided
// at level N are spread at level N+1.
// ─────────────────────────────────────────────────────────────────────────────
static const vector<int> HASH_PRIMES = {7, 11, 13, 17, 19, 23, 29, 31, 37, 41};
bool isInteger(const string &s)
{
    if (s.empty()) return false;
    size_t i = 0;
    if (s[0] == '-' || s[0] == '+') i = 1;
    for (; i < s.size(); i++)
        if (!isdigit(s[i])) return false;
    return true;
}
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    int found = 0;
    int count = 0;
    for (auto &tok : tokenizedQuery){
        if (!tok.empty() && tok.back() == ';'){
            tok.pop_back();
            found = 1;
            count++;
        }
    }
    if(count>1)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    if(found == 0)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    if (tokenizedQuery.size() < 9)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];

    string table1 = tokenizedQuery[3];
    string table2;
    int onIndex;

    // Case 1: token = "A,"  (comma attached)
    if (table1.back() == ',')
    {
        table1.pop_back();
        table2 = tokenizedQuery[4];
        onIndex = 5;
    }
    // Case 2: tokenized as A , B  (comma as separate token)
    else if (tokenizedQuery.size() > 5 && tokenizedQuery[4] == ",")
    {
        table2 = tokenizedQuery[5];
        onIndex = 6;
    }
    // Case 3: comma consumed by tokenizer
    else
    {
        table2 = tokenizedQuery[4];
        onIndex = 5;
    }

    parsedQuery.joinFirstRelationName  = table1;
    parsedQuery.joinSecondRelationName = table2;

    string tok1 = tokenizedQuery[onIndex + 1];
    string tok2 = tokenizedQuery[onIndex + 2];
    string tok3 = tokenizedQuery[onIndex + 3];

    auto splitDot = [](const string &s, string &tbl, string &col) -> bool {
        size_t dot = s.find('.');
        if (dot == string::npos) return false;
        tbl = s.substr(0, dot);
        col = s.substr(dot + 1);
        return true;
    };

    string lhsTable, lhsCol, rhsTable, rhsCol;
    uint nextIdx;

    if (tok2 == "==")
    {
        parsedQuery.joinConditionType  = JOIN_ATTR_EQUAL;
        parsedQuery.joinBinaryOperator = EQUAL;

        if (!splitDot(tok1, lhsTable, lhsCol) || !splitDot(tok3, rhsTable, rhsCol))
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        parsedQuery.joinFirstColumnName  = lhsCol;
        parsedQuery.joinSecondColumnName = rhsCol;
        nextIdx = onIndex + 4;
    }
    else if (tok2 == "+" || tok2 == "-")
    {
        if (tokenizedQuery.size() < (uint)(onIndex + 6))
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        string tok4 = tokenizedQuery[onIndex + 4];
        string tok5 = tokenizedQuery[onIndex + 5];

        if (tok4 != "==")
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }

        parsedQuery.joinConditionType  = JOIN_ARITH_EXPR;
        parsedQuery.joinBinaryOperator = EQUAL;
        parsedQuery.joinArithOp        = tok2;
        if (!tok5.empty() && tok5.back() == ';')
            tok5.pop_back();

        if (!isInteger(tok5))
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }

        parsedQuery.joinArithNumber = stoi(tok5);
        if (!splitDot(tok1, lhsTable, lhsCol) || !splitDot(tok3, rhsTable, rhsCol))
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        parsedQuery.joinFirstColumnName  = lhsCol;
        parsedQuery.joinSecondColumnName = rhsCol;
        nextIdx = onIndex + 6;
    }
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // ── Optional WHERE ────────────────────────────────────────────────────
    parsedQuery.joinHasWhere = false;
    if (nextIdx < tokenizedQuery.size() && tokenizedQuery[nextIdx] == "WHERE")
    {
        if (nextIdx + 3 >= tokenizedQuery.size())
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        string whereExpr = tokenizedQuery[nextIdx + 1];
        string whereOp   = tokenizedQuery[nextIdx + 2];
        string whereNum  = tokenizedQuery[nextIdx + 3];

        string wTable, wCol;
        if (!splitDot(whereExpr, wTable, wCol))
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        parsedQuery.joinWhereTable  = wTable;
        parsedQuery.joinWhereColumn = wCol;
        if (!whereNum.empty() && whereNum.back() == ';')
            whereNum.pop_back();

        if (!isInteger(whereNum))
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }

        parsedQuery.joinWhereNumber = stoi(whereNum);

        if (whereOp == "==")      parsedQuery.joinWhereBinaryOperator = EQUAL;
        else if (whereOp == "!=") parsedQuery.joinWhereBinaryOperator = NOT_EQUAL;
        else if (whereOp == ">")  parsedQuery.joinWhereBinaryOperator = GREATER_THAN;
        else if (whereOp == ">=") parsedQuery.joinWhereBinaryOperator = GEQ;
        else if (whereOp == "<")  parsedQuery.joinWhereBinaryOperator = LESS_THAN;
        else if (whereOp == "<=") parsedQuery.joinWhereBinaryOperator = LEQ;
        else { cout << "SYNTAX ERROR" << endl; return false; }

        parsedQuery.joinHasWhere = true;
        nextIdx += 4;
    }

    // ── Optional PROJECT ──────────────────────────────────────────────────
    parsedQuery.joinHasProject = false;
    parsedQuery.joinProjectList.clear();
    if (nextIdx < tokenizedQuery.size() && tokenizedQuery[nextIdx] == "PROJECT")
    {
        nextIdx++;
        while (nextIdx < tokenizedQuery.size())
        {
            string tok = tokenizedQuery[nextIdx];
            if (!tok.empty() && tok.back() == ',') tok.pop_back();
            if (!tok.empty() && tok.back() == ';') tok.pop_back();

            string pTable, pCol;
            if (!splitDot(tok, pTable, pCol))
            {
                cout << "SYNTAX ERROR" << endl;
                return false;
            }
            parsedQuery.joinProjectList.push_back({pTable, pCol});
            nextIdx++;
        }
        if (parsedQuery.joinProjectList.empty())
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        parsedQuery.joinHasProject = true;
    }

    return true;
}


bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) ||
        !tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName,
                                          parsedQuery.joinFirstRelationName) ||
        !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName,
                                          parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    if (parsedQuery.joinHasWhere)
    {
        if (parsedQuery.joinWhereTable != parsedQuery.joinFirstRelationName &&
            parsedQuery.joinWhereTable != parsedQuery.joinSecondRelationName)
        {
            cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
            return false;
        }
        if (!tableCatalogue.isColumnFromTable(parsedQuery.joinWhereColumn,
                                              parsedQuery.joinWhereTable))
        {
            cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
            return false;
        }
    }

    if (parsedQuery.joinHasProject)
    {
        for (auto &[tbl, col] : parsedQuery.joinProjectList)
        {
            if (tbl != parsedQuery.joinFirstRelationName &&
                tbl != parsedQuery.joinSecondRelationName)
            {
                cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
                return false;
            }
            if (!tableCatalogue.isColumnFromTable(col, tbl))
            {
                cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
                return false;
            }
        }
    }

    return true;
}


// ─────────────────────────────────────────────────────────────────────────────
// Helper: does partition p1 fit in B-2 blocks?
// We need B-2 blocks for the hash table, 1 for reading S, 1 for output buffer.
// ─────────────────────────────────────────────────────────────────────────────
static bool fitsInMemory(Table *t)
{
    int available = (int)MAX_BLOCKS_IN_MEMORY - 2;
    if (available < 1) available = 1;
    return (int)t->blockCount <= available;
}


// ─────────────────────────────────────────────────────────────────────────────
// Helper: apply WHERE + PROJECT to a combined row and write to result.
// Returns true if the row was emitted.
// ─────────────────────────────────────────────────────────────────────────────
static bool emitRow(const vector<int> &combined,
                    int whereColIdx,
                    const vector<int> &resultColSources,
                    Table *resultTable)
{
    if (parsedQuery.joinHasWhere)
    {
        int val = combined[whereColIdx];
        int num = parsedQuery.joinWhereNumber;
        bool pass = false;
        switch (parsedQuery.joinWhereBinaryOperator)
        {
            case EQUAL:        pass = val == num; break;
            case NOT_EQUAL:    pass = val != num; break;
            case GREATER_THAN: pass = val >  num; break;
            case GEQ:          pass = val >= num; break;
            case LESS_THAN:    pass = val <  num; break;
            case LEQ:          pass = val <= num; break;
            default:           pass = false;
        }
        if (!pass) return false;
    }

    vector<int> outRow;
    outRow.reserve(resultColSources.size());
    for (int idx : resultColSources)
        outRow.push_back(combined[idx]);

    resultTable->writeRow(outRow);
    return true;
}


// ─────────────────────────────────────────────────────────────────────────────
// Helper: partition a Table into numParts sub-tables using hash prime at
// position primeIdx in HASH_PRIMES. Sub-table names are prefix_0, prefix_1...
// Returns the names of the created sub-tables.
// ─────────────────────────────────────────────────────────────────────────────
static vector<string> partitionTable(Table *table,
                                     int joinColIdx,
                                     const string &prefix,
                                     int numParts,
                                     int primeIdx,
                                     int keyTransform=0,
                                     bool useTransform = false,
                                     bool transformAdd = false)
{
    int prime = HASH_PRIMES[primeIdx % (int)HASH_PRIMES.size()];

    vector<string> names;
    for (int i = 0; i < numParts; i++)
    {
        string name = prefix + "_" + to_string(i);
        names.push_back(name);
        tableCatalogue.insertTable(new Table(name, table->columns));
    }

    Cursor cursor = table->getCursor();
    vector<int> row = cursor.getNext();
    while (!row.empty())
    {
        int key = row[joinColIdx];
        // if (useTransform) key = keyTransform - key;
        if (useTransform) {
            key = transformAdd ? key + keyTransform : keyTransform - key;
        }
        int p = ((key % prime) + prime) % prime % numParts;
        Table *dest = tableCatalogue.getTable(names[p]);
        dest->writeRow(row);
        dest->rowCount++;
        row = cursor.getNext();
    }

    // Blockify each sub-partition so Cursor can read it
    for (auto &name : names)
        tableCatalogue.getTable(name)->blockify();

    return names;
}


// ─────────────────────────────────────────────────────────────────────────────
// Core recursive probe:
//   Given a pair of already-partitioned tables (rTable, sTable) that share
//   the same hash bucket, probe them. If rTable is too large, re-partition
//   both recursively using the next prime.
// ─────────────────────────────────────────────────────────────────────────────
static bool probeOrRecurse(Table *rTable, int rColIdx,
                           Table *sTable, int sColIdx,
                           int whereColIdx,
                           const vector<int> &resultColSources,
                           Table *resultTable,
                           int depth)
{
    bool produced = false;
    int numParts  = (int)MAX_BLOCKS_IN_MEMORY - 1;
    if (numParts < 1) numParts = 1;

    // ── Base case: rTable fits in memory → build hash and probe ──────────
    // if (parsedQuery.joinConditionType == JOIN_ARITH_EXPR || fitsInMemory(rTable))
    if (fitsInMemory(rTable))
    {
        // Build in-memory hash table from rTable
        unordered_map<int, vector<vector<int>>> hashTable;
        {
            Cursor cur = rTable->getCursor();
            vector<int> row = cur.getNext();
            while (!row.empty())
            {
                hashTable[row[rColIdx]].push_back(row);
                row = cur.getNext();
            }
        }

        // Probe with sTable
        Cursor cur = sTable->getCursor();
        vector<int> sRow = cur.getNext();
        while (!sRow.empty())
        {
            if (parsedQuery.joinConditionType == JOIN_ATTR_EQUAL)
            {
                int key = sRow[sColIdx];
                auto it = hashTable.find(key);
                if (it != hashTable.end())
                {
                    for (auto &rRow : it->second)
                    {
                        vector<int> combined = rRow;
                        combined.insert(combined.end(), sRow.begin(), sRow.end());
                        if (emitRow(combined, whereColIdx, resultColSources, resultTable))
                            produced = true;
                    }
                }
            }
            else // ARITH
            {
                for (auto &[k, rRows] : hashTable)
                {
                    for (auto &rRow : rRows)
                    {
                        int lhs = (parsedQuery.joinArithOp == "+")
                                  ? rRow[rColIdx] + sRow[sColIdx]
                                  : rRow[rColIdx] - sRow[sColIdx];
                        if (lhs != parsedQuery.joinArithNumber) continue;

                        vector<int> combined = rRow;
                        combined.insert(combined.end(), sRow.begin(), sRow.end());
                        if (emitRow(combined, whereColIdx, resultColSources, resultTable))
                            produced = true;
                    }
                }
            }
            sRow = cur.getNext();
        }
        return produced;
    }

    // ── Recursive case: rTable too large → re-partition both ─────────────
    // Use the next prime in the sequence to avoid same-bucket collisions
    int primeIdx = depth + 1; // depth 0 used prime[0] already

    string rPrefix = rTable->tableName + "_rec" + to_string(depth);
    string sPrefix = sTable->tableName + "_rec" + to_string(depth);

    vector<string> rSubNames = partitionTable(rTable, rColIdx, rPrefix, numParts, primeIdx);
    vector<string> sSubNames;
    if (parsedQuery.joinConditionType == JOIN_ARITH_EXPR)
    {
        if (parsedQuery.joinArithOp == "+")
            sSubNames = partitionTable(sTable, sColIdx, sPrefix, numParts, primeIdx,
                                    parsedQuery.joinArithNumber, true, false);
        else
            sSubNames = partitionTable(sTable, sColIdx, sPrefix, numParts, primeIdx,
                                    parsedQuery.joinArithNumber, true, true);
    }
    else
    {
        sSubNames = partitionTable(sTable, sColIdx, sPrefix, numParts, primeIdx);
    }

    for (int i = 0; i < numParts; i++)
    {
        Table *rSub = tableCatalogue.getTable(rSubNames[i]);
        Table *sSub = tableCatalogue.getTable(sSubNames[i]);

        if (rSub->rowCount == 0 || sSub->rowCount == 0)
        {
            tableCatalogue.deleteTable(rSubNames[i]);
            tableCatalogue.deleteTable(sSubNames[i]);
            continue;
        }

        if (probeOrRecurse(rSub, rColIdx, sSub, sColIdx,
                           whereColIdx, resultColSources,
                           resultTable, depth + 1))
            produced = true;

        tableCatalogue.deleteTable(rSubNames[i]);
        tableCatalogue.deleteTable(sSubNames[i]);
    }

    return produced;
}


// ─────────────────────────────────────────────────────────────────────────────
// executeJOIN
// ─────────────────────────────────────────────────────────────────────────────
void executeJOIN()
{
    logger.log("executeJOIN");

    Table *table1 = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table *table2 = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);

    int colIndex1 = table1->getColumnIndex(parsedQuery.joinFirstColumnName);
    int colIndex2 = table2->getColumnIndex(parsedQuery.joinSecondColumnName);

    int rCols = table1->columnCount;
    int sCols = table2->columnCount;

    int effectivePartitions = (int)MAX_BLOCKS_IN_MEMORY - 1;
    if (effectivePartitions < 1) effectivePartitions = 1;

    // ── Build result column list ──────────────────────────────────────────
    vector<string> resultColumns;
    vector<int>    resultColSources;

    if (parsedQuery.joinHasProject)
    {
        for (auto &[tbl, col] : parsedQuery.joinProjectList)
        {
            resultColumns.push_back(col);
            if (tbl == parsedQuery.joinFirstRelationName)
                resultColSources.push_back(table1->getColumnIndex(col));
            else
                resultColSources.push_back(rCols + table2->getColumnIndex(col));
        }
    }
    else
    {
        for (int i = 0; i < rCols; i++) resultColSources.push_back(i);
        for (int i = 0; i < sCols; i++) resultColSources.push_back(rCols + i);
        resultColumns = table1->columns;
        for (auto &c : table2->columns) resultColumns.push_back(c);
    }

    // WHERE column index in combined [R | S] row
    int whereColIdx = -1;
    if (parsedQuery.joinHasWhere)
    {
        if (parsedQuery.joinWhereTable == parsedQuery.joinFirstRelationName)
            whereColIdx = table1->getColumnIndex(parsedQuery.joinWhereColumn);
        else
            whereColIdx = rCols + table2->getColumnIndex(parsedQuery.joinWhereColumn);
    }

    // ── Phase 1: Partition both tables ────────────────────────────────────
    string p1Prefix = parsedQuery.joinFirstRelationName  + "_part";
    string p2Prefix = parsedQuery.joinSecondRelationName + "_part";

    vector<string> part1Names = partitionTable(table1, colIndex1, p1Prefix,
                                               effectivePartitions, 0);
    vector<string> part2Names;

    if (parsedQuery.joinConditionType == JOIN_ARITH_EXPR)
    {
        if (parsedQuery.joinArithOp == "+")
            part2Names = partitionTable(table2, colIndex2, p2Prefix,
                                        effectivePartitions, 0,
                                        parsedQuery.joinArithNumber,
                                        true, false);
        else
            part2Names = partitionTable(table2, colIndex2, p2Prefix,
                                        effectivePartitions, 0,
                                        parsedQuery.joinArithNumber,
                                        true, true);
    }
    else
    {
        part2Names = partitionTable(table2, colIndex2, p2Prefix,
                                    effectivePartitions, 0);
    }

    // ── Phase 2: For each partition pair, probe or recurse ────────────────
    Table *resultTable = new Table(parsedQuery.joinResultRelationName, resultColumns);
    bool producedRows  = false;

    for (int i = 0; i < effectivePartitions; i++)
    {
        Table *p1 = tableCatalogue.getTable(part1Names[i]);
        Table *p2 = tableCatalogue.getTable(part2Names[i]);

        if (p1->rowCount == 0 || p2->rowCount == 0)
        {
            tableCatalogue.deleteTable(part1Names[i]);
            tableCatalogue.deleteTable(part2Names[i]);
            continue;
        }

        if (probeOrRecurse(p1, colIndex1, p2, colIndex2,
                           whereColIdx, resultColSources,
                           resultTable, 0))
            producedRows = true;

        tableCatalogue.deleteTable(part1Names[i]);
        tableCatalogue.deleteTable(part2Names[i]);
    }

    // ── Register result ───────────────────────────────────────────────────
    if (!producedRows)
    {
        cout << "No rows satisfy join condition" << endl;
        delete resultTable;
        return;
    }
    resultTable->blockify();

    tableCatalogue.insertTable(resultTable);

}