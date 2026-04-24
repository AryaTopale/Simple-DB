#include "../global.h"
#include <cstdio>

/* ------------------------------------------------------------------ */
/*  Custom stable sort (no std::sort)                                 */
/* ------------------------------------------------------------------ */

template <typename T, typename Compare>
static void mergeInto(vector<T> &arr, vector<T> &buffer, int left, int mid, int right, Compare cmp)
{
    int i = left;
    int j = mid;
    int k = left;
    while (i < mid && j < right)
    {
        if (cmp(arr[j], arr[i]))
            buffer[k++] = arr[j++];
        else
            buffer[k++] = arr[i++];
    }
    while (i < mid)
        buffer[k++] = arr[i++];
    while (j < right)
        buffer[k++] = arr[j++];
    for (int t = left; t < right; t++)
        arr[t] = buffer[t];
}

template <typename T, typename Compare>
static void stableMergeSortImpl(vector<T> &arr, vector<T> &buffer, int left, int right, Compare cmp)
{
    if (right - left <= 1)
        return;
    int mid = left + (right - left) / 2;
    stableMergeSortImpl(arr, buffer, left, mid, cmp);
    stableMergeSortImpl(arr, buffer, mid, right, cmp);
    mergeInto(arr, buffer, left, mid, right, cmp);
}

template <typename T, typename Compare>
static void stableMergeSort(vector<T> &arr, Compare cmp)
{
    if (arr.size() <= 1)
        return;
    vector<T> buffer(arr.size());
    stableMergeSortImpl(arr, buffer, 0, (int)arr.size(), cmp);
}
#include <climits>
#include <regex>
#include <unordered_map>

/**
 * GROUP BY with HAVING clause.
 *
 * SYNTAX (all on one line):
 * R1, R2 <- GROUP BY A, B FROM T HAVING AGG(col) op AGG(col)|number RETURN AGG(col), AGG(col)
 *
 * The tokenizer (regex [^\s,]+) splits on whitespace and commas, so commas
 * never appear as separate tokens nor attached to tokens.
 *
 * EXTERNAL-MEMORY STRATEGY:
 * Two-phase partition-based (hash) aggregation:
 *
 *   Phase 1 – Partition: Scan the source table row-by-row via a Cursor.
 *     For each row, hash the group key into one of P partitions.
 *     Each partition is flushed to temp pages via bufferManager when its
 *     in-memory buffer fills one page (projMaxRows rows).
 *     Memory at any point: 1 page in Cursor + at most 1 page per partition
 *     = 1 + numPartitions <= MAX_BLOCKS_IN_MEMORY.
 *
 *   Phase 2 – Per-partition aggregation: For each partition, read it back
 *     page-by-page via bufferManager.getTempPage(), build an in-memory
 *     hash map of group-key → accumulators, then evaluate HAVING and
 *     produce result rows.
 *     Memory at any point: 1 page (read buffer) + groupMap (working memory).
 *     numPartitions is sized so each partition's rows fit in
 *     (MAX_BLOCKS_IN_MEMORY - 2) pages worth of rows.
 *
 *   Phase 3 – Write results: Write result rows page-by-page via bufferManager.
 */

/* ===================================================================
 *  Helper: parse an aggregate expression token like "MAX(Salary)"
 *  or "COUNT(*)" into an AggregateExpr.
 * =================================================================== */
static bool parseAggregateExpr(const string &token, AggregateExpr &expr)
{
    size_t lp = token.find('(');
    size_t rp = token.find(')');
    if (lp == string::npos || rp == string::npos || rp <= lp + 1)
        return false;

    string funcName = token.substr(0, lp);
    string colName  = token.substr(lp + 1, rp - lp - 1);

    if      (funcName == "MAX")   expr.func = MAX_AGG;
    else if (funcName == "MIN")   expr.func = MIN_AGG;
    else if (funcName == "COUNT") expr.func = COUNT_AGG;
    else if (funcName == "SUM")   expr.func = SUM_AGG;
    else if (funcName == "AVG")   expr.func = AVG_AGG;
    else return false;

    expr.columnName = colName;
    return true;
}

/* ===================================================================
 *  Helper: parse a BinaryOperator from a string token.
 * =================================================================== */
static bool parseBinOp(const string &token, BinaryOperator &op)
{
    if      (token == "==") op = EQUAL;
    else if (token == "!=") op = NOT_EQUAL;
    else if (token == ">")  op = GREATER_THAN;
    else if (token == ">=") op = GEQ;
    else if (token == "<")  op = LESS_THAN;
    else if (token == "<=") op = LEQ;
    else return false;
    return true;
}

/* ===================================================================
 *  SYNTACTIC PARSE
 * =================================================================== */
bool syntacticParseGROUP_BY()
{
    logger.log("syntacticParseGROUP_BY");

    /* --- 1. Find the position of "<-" --- */
    int arrowPos = -1;
    for (int i = 0; i < (int)tokenizedQuery.size(); i++)
    {
        if (tokenizedQuery[i] == "<-") { arrowPos = i; break; }
    }
    if (arrowPos < 1)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    /* --- 2. Everything before "<-" is a result table name --- */
    for (int i = 0; i < arrowPos; i++)
        parsedQuery.groupByResultRelations.push_back(tokenizedQuery[i]);

    if (parsedQuery.groupByResultRelations.empty())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    /* --- 3. Expect "GROUP" "BY" after "<-" --- */
    int idx = arrowPos + 1;
    if (idx + 1 >= (int)tokenizedQuery.size() ||
        tokenizedQuery[idx] != "GROUP" || tokenizedQuery[idx + 1] != "BY")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    idx += 2; // skip GROUP BY

    /* --- 4. Collect grouping attributes until "FROM" --- */
    while (idx < (int)tokenizedQuery.size() && tokenizedQuery[idx] != "FROM")
    {
        parsedQuery.groupByGroupAttrs.push_back(tokenizedQuery[idx]);
        idx++;
    }
    if (parsedQuery.groupByGroupAttrs.empty() || idx >= (int)tokenizedQuery.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    idx++; // skip "FROM"

    /* --- 5. Read source table name --- */
    if (idx >= (int)tokenizedQuery.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.groupBySourceRelation = tokenizedQuery[idx];
    idx++;

    /* --- 6. Parse HAVING clause --- */
    if (idx >= (int)tokenizedQuery.size() || tokenizedQuery[idx] != "HAVING")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    idx++; // skip "HAVING"

    // LHS aggregate
    if (idx >= (int)tokenizedQuery.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    if (!parseAggregateExpr(tokenizedQuery[idx], parsedQuery.groupByHavingLHS))
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    idx++;

    // Operator
    if (idx >= (int)tokenizedQuery.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    if (!parseBinOp(tokenizedQuery[idx], parsedQuery.groupByHavingOp))
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    idx++;

    // RHS: either an aggregate or a number
    if (idx >= (int)tokenizedQuery.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    if (tokenizedQuery[idx].find('(') != string::npos)
    {
        parsedQuery.groupByHavingRHSIsAggregate = true;
        if (!parseAggregateExpr(tokenizedQuery[idx], parsedQuery.groupByHavingRHS))
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }
    else
    {
        parsedQuery.groupByHavingRHSIsAggregate = false;
        try
        {
            parsedQuery.groupByHavingRHSValue = stoi(tokenizedQuery[idx]);
        }
        catch (...)
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }
    idx++;

    /* --- 7. Parse RETURN clause --- */
    if (idx >= (int)tokenizedQuery.size() || tokenizedQuery[idx] != "RETURN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    idx++; // skip "RETURN"

    while (idx < (int)tokenizedQuery.size())
    {
        AggregateExpr expr;
        if (!parseAggregateExpr(tokenizedQuery[idx], expr))
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        parsedQuery.groupByReturnAggregates.push_back(expr);
        idx++;
    }

    if (parsedQuery.groupByReturnAggregates.empty())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    /* --- 8. Validate counts match --- */
    if (parsedQuery.groupByResultRelations.size() != parsedQuery.groupByGroupAttrs.size() ||
        parsedQuery.groupByGroupAttrs.size()       != parsedQuery.groupByReturnAggregates.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = GROUP_BY;
    return true;
}

/* ===================================================================
 *  SEMANTIC PARSE
 * =================================================================== */
bool semanticParseGROUP_BY()
{
    logger.log("semanticParseGROUP_BY");

    // 1. Source table must exist
    if (!tableCatalogue.isTable(parsedQuery.groupBySourceRelation))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    // 2. Each grouping attribute must exist in the source table
    for (auto &attr : parsedQuery.groupByGroupAttrs)
    {
        if (!tableCatalogue.isColumnFromTable(attr, parsedQuery.groupBySourceRelation))
        {
            cout << "SEMANTIC ERROR: Group By column doesn't exist in relation" << endl;
            return false;
        }
    }

    // 3. HAVING columns must exist
    if (parsedQuery.groupByHavingLHS.columnName != "*" &&
        !tableCatalogue.isColumnFromTable(parsedQuery.groupByHavingLHS.columnName,
                                          parsedQuery.groupBySourceRelation))
    {
        cout << "SEMANTIC ERROR: Having column doesn't exist in relation" << endl;
        return false;
    }
    if (parsedQuery.groupByHavingRHSIsAggregate &&
        parsedQuery.groupByHavingRHS.columnName != "*" &&
        !tableCatalogue.isColumnFromTable(parsedQuery.groupByHavingRHS.columnName,
                                          parsedQuery.groupBySourceRelation))
    {
        cout << "SEMANTIC ERROR: Having column doesn't exist in relation" << endl;
        return false;
    }

    // 4. RETURN columns must exist
    for (auto &agg : parsedQuery.groupByReturnAggregates)
    {
        if (agg.columnName != "*" &&
            !tableCatalogue.isColumnFromTable(agg.columnName, parsedQuery.groupBySourceRelation))
        {
            cout << "SEMANTIC ERROR: Return column doesn't exist in relation" << endl;
            return false;
        }
    }

    return true;
}

/* ===================================================================
 *  EXECUTOR – External-memory two-phase hash aggregation
 * =================================================================== */

/* Per-column accumulator: tracks running values so any aggregate can be
   derived at the end. */
struct ColAccum {
    long long sum   = 0;
    int       count = 0;
    int       minVal = INT_MAX;
    int       maxVal = INT_MIN;
};

/* Compute final aggregate value from an accumulator. */
static int computeAggregate(AggregateFunction func, const ColAccum &acc)
{
    switch (func)
    {
    case MAX_AGG:   return acc.maxVal;
    case MIN_AGG:   return acc.minVal;
    case COUNT_AGG: return acc.count;
    case SUM_AGG:   return (int)acc.sum;
    case AVG_AGG:   return (acc.count == 0) ? 0 : (int)(acc.sum / acc.count);
    default:        return 0;
    }
}

/* Update an accumulator with a new value. */
static void updateAccum(ColAccum &acc, int value)
{
    acc.sum += value;
    acc.count++;
    if (value < acc.minVal) acc.minVal = value;
    if (value > acc.maxVal) acc.maxVal = value;
}

/* Build the result column name for a RETURN aggregate.
   E.g. MAX(Salary) -> "MAXSalary", COUNT(*) -> "COUNT". */
static string makeResultColumnName(const AggregateExpr &agg)
{
    if (agg.columnName == "*")
        return "COUNT";
    string funcStr;
    switch (agg.func)
    {
    case MAX_AGG:   funcStr = "MAX";   break;
    case MIN_AGG:   funcStr = "MIN";   break;
    case COUNT_AGG: funcStr = "COUNT"; break;
    case SUM_AGG:   funcStr = "SUM";   break;
    case AVG_AGG:   funcStr = "AVG";   break;
    default:        funcStr = "";      break;
    }
    return funcStr + agg.columnName;
}

/* Collect the set of distinct column names that we need to accumulate
   (for HAVING + RETURN for a given grouping index).
   Returns column names excluding "*" (handled via the star accumulator). */
static vector<string> columnsToAccumulate(int groupIdx)
{
    vector<string> cols;
    auto addUnique = [&](const string &c) {
        if (c == "*") return;
        for (auto &x : cols) if (x == c) return;
        cols.push_back(c);
    };

    addUnique(parsedQuery.groupByHavingLHS.columnName);
    if (parsedQuery.groupByHavingRHSIsAggregate)
        addUnique(parsedQuery.groupByHavingRHS.columnName);
    addUnique(parsedQuery.groupByReturnAggregates[groupIdx].columnName);

    return cols;
}

void executeGROUP_BY()
{
    logger.log("executeGROUP_BY");

    Table *srcTable = tableCatalogue.getTable(parsedQuery.groupBySourceRelation);
    int numGroups   = (int)parsedQuery.groupByGroupAttrs.size();
    string srcName  = srcTable->tableName;

    /* ------------------------------------------------------------------
     *  Pre-compute column indices we will need from each source row.
     * ------------------------------------------------------------------ */
    vector<int> groupColIdx(numGroups);
    // colsNeeded[i] = list of (colName, colIndex) pairs for accumulation
    vector<vector<pair<string,int>>> colsNeeded(numGroups);

    for (int i = 0; i < numGroups; i++)
    {
        groupColIdx[i] = srcTable->getColumnIndex(parsedQuery.groupByGroupAttrs[i]);
        for (auto &c : columnsToAccumulate(i))
            colsNeeded[i].push_back({c, srcTable->getColumnIndex(c)});
    }

    

    struct TempRunMeta
    {
        // rowsPerPage[i] = number of rows stored in temp page i
        vector<int> rowsPerPage;
        int totalRows = 0;
    };

    // Flush helper for a (temp) page buffer.
    auto flushTempPage = [&](const string &pageName,
                             vector<vector<int>> &buf,
                             TempRunMeta &meta)
    {
        if (buf.empty())
            return;
        bufferManager.writeTempPage(pageName, buf, (int)buf.size());
        meta.rowsPerPage.push_back((int)buf.size());
        meta.totalRows += (int)buf.size();
        buf.clear();
    };

    // External sort the temp pages [prefix + "_Page<i>"] by column 0
    // into a new set of temp pages [outPrefix + "_Page<i>"].
    // Returns metadata about the sorted run.
    auto externalSortTempByKey = [&](const string &inPrefix,
                                     const TempRunMeta &inMeta,
                                     int columnCount,
                                     int maxRowsPerPage,
                                     const string &outPrefix) -> TempRunMeta
    {
        int K = (int)MAX_BLOCKS_IN_MEMORY;
        if (K < 3)
            K = 3; // ensure at least 2-way merge + output

        struct RunCursor
        {
            int runIdx;
            int pageInRun;
            int rowInPage;
            int totalPages;
            Page page;
            int pageRows;
            bool exhausted;
        };

        // Phase 0: create initial sorted runs by reading up to K pages.
        int numRuns = 0;
        vector<vector<int>> runRowsPerPage;

        for (int p = 0; p < (int)inMeta.rowsPerPage.size();)
        {
            vector<vector<int>> chunk;
            chunk.reserve((size_t)K * (size_t)maxRowsPerPage);

            int pagesRead = 0;
            while (p < (int)inMeta.rowsPerPage.size() && pagesRead < K)
            {
                string pageName = "../data/temp/" + inPrefix + "_Page" + to_string(p);
                int rowsInPage = inMeta.rowsPerPage[p];
                Page page = bufferManager.getTempPage(pageName, columnCount, rowsInPage);
                for (int r = 0; r < rowsInPage; r++)
                    chunk.push_back(page.getRow(r));
                p++;
                pagesRead++;
            }

            if (chunk.empty())
                continue;

            stableMergeSort(chunk, [&](const vector<int> &a, const vector<int> &b) {
                return a[0] < b[0];
            });

            int pagesInRun = 0;
            vector<int> rowsPP;
            for (int i = 0; i < (int)chunk.size();)
            {
                int end = min(i + maxRowsPerPage, (int)chunk.size());
                vector<vector<int>> outBuf(chunk.begin() + i, chunk.begin() + end);
                string outName = "../data/temp/" + outPrefix + "_R" + to_string(numRuns) + "_Page" + to_string(pagesInRun);
                bufferManager.writeTempPage(outName, outBuf, (int)outBuf.size());
                rowsPP.push_back((int)outBuf.size());
                pagesInRun++;
                i = end;
            }
            runRowsPerPage.push_back(rowsPP);
            numRuns++;
        }

        // Merge runs until one remains.
        int mergeWays = max(K - 1, 2);
        string workPrefix = outPrefix;
        int phase = 0;

        while (numRuns > 1)
        {
            int newNumRuns = 0;
            vector<vector<int>> newRunRowsPerPage;
            string nextPrefix = outPrefix + "_M" + to_string(phase);

            for (int r0 = 0; r0 < numRuns; r0 += mergeWays)
            {
                int runsToMerge = min(mergeWays, numRuns - r0);
                if (runsToMerge == 1)
                {
                    // Rename/forward pages into next namespace.
                    int srcRun = r0;
                    int dstRun = newNumRuns;
                    vector<int> newRowsPP;
                    for (int p = 0; p < (int)runRowsPerPage[srcRun].size(); p++)
                    {
                        string srcName = "../data/temp/" + workPrefix + "_R" + to_string(srcRun) + "_Page" + to_string(p);
                        string dstName = "../data/temp/" + nextPrefix + "_R" + to_string(dstRun) + "_Page" + to_string(p);
                        rename(srcName.c_str(), dstName.c_str());
                        newRowsPP.push_back(runRowsPerPage[srcRun][p]);
                    }
                    newRunRowsPerPage.push_back(newRowsPP);
                    newNumRuns++;
                    continue;
                }

                vector<RunCursor> cursors(runsToMerge);
                for (int i = 0; i < runsToMerge; i++)
                {
                    int ri = r0 + i;
                    cursors[i].runIdx = ri;
                    cursors[i].pageInRun = 0;
                    cursors[i].rowInPage = 0;
                    cursors[i].totalPages = (int)runRowsPerPage[ri].size();
                    cursors[i].exhausted = (cursors[i].totalPages == 0);
                    if (!cursors[i].exhausted)
                    {
                        string pageName = "../data/temp/" + workPrefix + "_R" + to_string(ri) + "_Page0";
                        int rowsInPage = runRowsPerPage[ri][0];
                        cursors[i].page = bufferManager.getTempPage(pageName, columnCount, rowsInPage);
                        cursors[i].pageRows = rowsInPage;
                    }
                }

                auto pqCmp = [&](int a, int b) -> bool {
                    vector<int> ra = cursors[a].page.getRow(cursors[a].rowInPage);
                    vector<int> rb = cursors[b].page.getRow(cursors[b].rowInPage);
                    if (ra[0] != rb[0])
                        return ra[0] > rb[0]; // min-heap
                    return a > b;
                };

                priority_queue<int, vector<int>, decltype(pqCmp)> pq(pqCmp);
                for (int i = 0; i < runsToMerge; i++)
                    if (!cursors[i].exhausted)
                        pq.push(i);

                vector<vector<int>> outputBuffer;
                int outputPageIdx = 0;
                vector<int> newRowsPP;
                int dstRun = newNumRuns;

                auto flushOut = [&]() {
                    if (outputBuffer.empty())
                        return;
                    string outName = "../data/temp/" + nextPrefix + "_R" + to_string(dstRun) + "_Page" + to_string(outputPageIdx);
                    bufferManager.writeTempPage(outName, outputBuffer, (int)outputBuffer.size());
                    newRowsPP.push_back((int)outputBuffer.size());
                    outputPageIdx++;
                    outputBuffer.clear();
                };

                while (!pq.empty())
                {
                    int ci = pq.top();
                    pq.pop();

                    outputBuffer.push_back(cursors[ci].page.getRow(cursors[ci].rowInPage));
                    cursors[ci].rowInPage++;

                    if (cursors[ci].rowInPage >= cursors[ci].pageRows)
                    {
                        cursors[ci].pageInRun++;
                        if (cursors[ci].pageInRun >= cursors[ci].totalPages)
                        {
                            cursors[ci].exhausted = true;
                        }
                        else
                        {
                            string pageName = "../data/temp/" + workPrefix + "_R" + to_string(cursors[ci].runIdx) + "_Page" + to_string(cursors[ci].pageInRun);
                            int rowsInPage = runRowsPerPage[cursors[ci].runIdx][cursors[ci].pageInRun];
                            cursors[ci].page = bufferManager.getTempPage(pageName, columnCount, rowsInPage);
                            cursors[ci].pageRows = rowsInPage;
                            cursors[ci].rowInPage = 0;
                        }
                    }

                    if (!cursors[ci].exhausted)
                        pq.push(ci);

                    if ((int)outputBuffer.size() >= maxRowsPerPage)
                        flushOut();
                }
                flushOut();
                newRunRowsPerPage.push_back(newRowsPP);
                newNumRuns++;
            }

            // Delete previous phase run pages to keep temp clean.
            for (int r = 0; r < numRuns; r++)
            {
                for (int p = 0; p < (int)runRowsPerPage[r].size(); p++)
                {
                    string name = "../data/temp/" + workPrefix + "_R" + to_string(r) + "_Page" + to_string(p);
                    bufferManager.deleteFile(name);
                }
            }

            runRowsPerPage = move(newRunRowsPerPage);
            numRuns = newNumRuns;
            workPrefix = nextPrefix;
            phase++;
        }

        // Final run is workPrefix_R0_Page<i>. Rename into outPrefix_Page<i>.
        TempRunMeta outMeta;
        if (numRuns == 1 && !runRowsPerPage.empty())
        {
            for (int p = 0; p < (int)runRowsPerPage[0].size(); p++)
            {
                string srcName = "../data/temp/" + workPrefix + "_R0_Page" + to_string(p);
                string dstName = "../data/temp/" + outPrefix + "_Page" + to_string(p);
                rename(srcName.c_str(), dstName.c_str());
                outMeta.rowsPerPage.push_back(runRowsPerPage[0][p]);
                outMeta.totalRows += runRowsPerPage[0][p];
            }
        }
        return outMeta;
    };

    // Stream-aggregate an already materialized temp relation by sorting by key
    // and then scanning. Writes result pages directly to resultTable.
    auto aggregateTempPartitionToResult = [&](const string &inPrefix,
                                             const TempRunMeta &inMeta,
                                             int projCols,
                                             int projMaxRows,
                                             int starIdx,
                                             int havLhsIdx,
                                             int havRhsIdx,
                                             int retIdx,
                                             const AggregateExpr &retAgg,
                                             Table *resultTable)
    {
        if (inMeta.totalRows == 0)
            return;

        string sortedPrefix = inPrefix + "_SORT";
        TempRunMeta sortedMeta = externalSortTempByKey(inPrefix, inMeta, projCols, projMaxRows, sortedPrefix);

        vector<vector<int>> outBuf;
        outBuf.reserve((size_t)resultTable->maxRowsPerBlock);

        int currentKey = 0;
        vector<ColAccum> accums;
        bool hasKey = false;

        auto emitGroup = [&]() {
            if (!hasKey)
                return;

            int lhsVal = computeAggregate(parsedQuery.groupByHavingLHS.func, accums[havLhsIdx]);
            int rhsVal = parsedQuery.groupByHavingRHSIsAggregate
                             ? computeAggregate(parsedQuery.groupByHavingRHS.func, accums[havRhsIdx])
                             : parsedQuery.groupByHavingRHSValue;

            if (evaluateBinOp(lhsVal, rhsVal, parsedQuery.groupByHavingOp))
            {
                int retVal = computeAggregate(retAgg.func, accums[retIdx]);
                outBuf.push_back({currentKey, retVal});
            }

            if ((int)outBuf.size() >= resultTable->maxRowsPerBlock)
            {
                int pageIdx = resultTable->blockCount;
                bufferManager.writePage(resultTable->tableName, pageIdx, outBuf, (int)outBuf.size());
                resultTable->rowsPerBlockCount.push_back((int)outBuf.size());
                resultTable->blockCount++;
                resultTable->rowCount += (int)outBuf.size();
                outBuf.clear();
            }
        };

        // Scan sorted pages.
        for (int pg = 0; pg < (int)sortedMeta.rowsPerPage.size(); pg++)
        {
            string pageName = "../data/temp/" + sortedPrefix + "_Page" + to_string(pg);
            int rowsInPage = sortedMeta.rowsPerPage[pg];
            Page page = bufferManager.getTempPage(pageName, projCols, rowsInPage);
            for (int r = 0; r < rowsInPage; r++)
            {
                vector<int> row = page.getRow(r);
                int key = row[0];
                if (!hasKey || key != currentKey)
                {
                    emitGroup();
                    currentKey = key;
                    hasKey = true;
                    accums.assign(starIdx + 1, ColAccum{});
                }

                for (int c = 0; c < starIdx; c++)
                    updateAccum(accums[c], row[1 + c]);
                accums[starIdx].count++;
            }
        }
        emitGroup();

        // Flush any remaining output buffer.
        if (!outBuf.empty())
        {
            int pageIdx = resultTable->blockCount;
            bufferManager.writePage(resultTable->tableName, pageIdx, outBuf, (int)outBuf.size());
            resultTable->rowsPerBlockCount.push_back((int)outBuf.size());
            resultTable->blockCount++;
            resultTable->rowCount += (int)outBuf.size();
            outBuf.clear();
        }

        // Delete sorted temp pages.
        for (int pg = 0; pg < (int)sortedMeta.rowsPerPage.size(); pg++)
        {
            string pageName = "../data/temp/" + sortedPrefix + "_Page" + to_string(pg);
            bufferManager.deleteFile(pageName);
        }
    };

    // Recursive partition-and-aggregate for a temp relation or the source table.
    // The input is provided as a row stream callback that yields rows in the
    // projected schema: [groupKey, accumCol0, ...]. It materializes partitions
    // as temp pages under partitionPrefix and then either recurses or aggregates.
    function<void(function<bool(vector<int>&)>, int, const string &, Table*,
                  int, int, int, int, int, int, const AggregateExpr &)> processStream;

    processStream = [&](function<bool(vector<int>&)> nextRow,
                        int recursionDepth,
                        const string &partitionPrefix,
                        Table *resultTable,
                        int projCols,
                        int projMaxRows,
                        int starIdx,
                        int havLhsIdx,
                        int havRhsIdx,
                        int retIdx,
                        const AggregateExpr &retAgg)
    {
        // If recursion gets too deep, fall back to sort-aggregate with 1 partition.
        if (recursionDepth > 8)
        {
            TempRunMeta meta;
            vector<vector<int>> buf;
            buf.reserve((size_t)projMaxRows);
            int pageIdx = 0;
            vector<int> row;
            while (nextRow(row))
            {
                buf.push_back(row);
                if ((int)buf.size() >= projMaxRows)
                {
                    string pageName = "../data/temp/" + partitionPrefix + "_Page" + to_string(pageIdx++);
                    flushTempPage(pageName, buf, meta);
                }
            }
            if (!buf.empty())
            {
                string pageName = "../data/temp/" + partitionPrefix + "_Page" + to_string(pageIdx++);
                flushTempPage(pageName, buf, meta);
            }
            aggregateTempPartitionToResult(partitionPrefix, meta, projCols, projMaxRows, starIdx,
                                           havLhsIdx, havRhsIdx, retIdx,
                                           retAgg, resultTable);
            // Delete materialized pages
            for (int pg = 0; pg < (int)meta.rowsPerPage.size(); pg++)
            {
                string pageName = "../data/temp/" + partitionPrefix + "_Page" + to_string(pg);
                bufferManager.deleteFile(pageName);
            }
            return;
        }

        // Compute partitioning parameters.
        int pagesForWork = max(1, (int)MAX_BLOCKS_IN_MEMORY - 2);
        long long rowsPerPartition = (long long)pagesForWork * projMaxRows;
        int maxPartitions = max(1, (int)MAX_BLOCKS_IN_MEMORY - 1);

        // Start with as many partitions as we can afford.
        int numPartitions = maxPartitions;

        vector<vector<vector<int>>> partBufs(numPartitions);
        vector<int> partPageCount(numPartitions, 0);
        vector<TempRunMeta> partMeta(numPartitions);

        for (int p = 0; p < numPartitions; p++)
            partBufs[p].reserve((size_t)projMaxRows);

        auto flushPartition = [&](int p) {
            if (partBufs[p].empty())
                return;
            string pageName = "../data/temp/" + partitionPrefix + "_D" + to_string(recursionDepth) + "_P" + to_string(p) + "_Page" + to_string(partPageCount[p]);
            bufferManager.writeTempPage(pageName, partBufs[p], (int)partBufs[p].size());
            partMeta[p].rowsPerPage.push_back((int)partBufs[p].size());
            partMeta[p].totalRows += (int)partBufs[p].size();
            partPageCount[p]++;
            partBufs[p].clear();
        };

        // Partition the stream.
        vector<int> row;
        while (nextRow(row))
        {
            int key = row[0];
            int p = (int)(((unsigned int)abs(key) + (unsigned int)(recursionDepth * 2654435761u)) % (unsigned int)numPartitions);
            partBufs[p].push_back(row);
            if ((int)partBufs[p].size() >= projMaxRows)
                flushPartition(p);
        }
        for (int p = 0; p < numPartitions; p++)
            flushPartition(p);

        // Process partitions: recurse if too big; else sort-aggregate.
        for (int p = 0; p < numPartitions; p++)
        {
            if (partMeta[p].totalRows == 0)
                continue;

            string partPrefix = partitionPrefix + "_D" + to_string(recursionDepth) + "_P" + to_string(p);

            if ((long long)partMeta[p].totalRows > rowsPerPartition)
            {
                // Recursively repartition this partition.
                int curPage = 0;
                int curRow = 0;
                Page cur;
                int curRowsInPage = 0;
                bool loaded = false;

                auto nextFromPart = [&](vector<int> &out) -> bool {
                    while (curPage < (int)partMeta[p].rowsPerPage.size())
                    {
                        if (!loaded)
                        {
                            string pageName = "../data/temp/" + partPrefix + "_Page" + to_string(curPage);
                            curRowsInPage = partMeta[p].rowsPerPage[curPage];
                            cur = bufferManager.getTempPage(pageName, projCols, curRowsInPage);
                            curRow = 0;
                            loaded = true;
                        }
                        if (curRow < curRowsInPage)
                        {
                            out = cur.getRow(curRow++);
                            return true;
                        }
                        loaded = false;
                        curPage++;
                    }
                    return false;
                };

                processStream(nextFromPart, recursionDepth + 1, partPrefix + "_R", resultTable,
                              projCols, projMaxRows, starIdx, havLhsIdx, havRhsIdx, retIdx, retAgg);
            }
            else
            {
                // Base case: sort + stream aggregate.
                aggregateTempPartitionToResult(partPrefix, partMeta[p], projCols, projMaxRows,
                                               starIdx, havLhsIdx, havRhsIdx, retIdx,
                                               retAgg, resultTable);
            }

            // Delete this partition's materialized pages.
            for (int pg = 0; pg < (int)partMeta[p].rowsPerPage.size(); pg++)
            {
                string pageName = "../data/temp/" + partPrefix + "_Page" + to_string(pg);
                bufferManager.deleteFile(pageName);
            }
        }
    };

    /* ------------------------------------------------------------------
     *  Process each grouping attribute independently.
     * ------------------------------------------------------------------ */
    for (int gi = 0; gi < numGroups; gi++)
    {
        string       resultName = parsedQuery.groupByResultRelations[gi];
        string       groupAttr  = parsedQuery.groupByGroupAttrs[gi];
        AggregateExpr &retAgg   = parsedQuery.groupByReturnAggregates[gi];

        string col1Name = groupAttr;
        string col2Name = makeResultColumnName(retAgg);

        int gci = groupColIdx[gi]; // column index of the grouping attribute

        /* --------------------------------------------------------------
         *  Projected column layout stored in partition pages:
         *    [groupKey, accumCol0, accumCol1, ...]
         *  projCols  = 1 (groupKey) + number of accumulation columns
         *  projMaxRows = how many projected rows fit in one page
         * ------------------------------------------------------------- */
        int projCols    = 1 + (int)colsNeeded[gi].size();
        int projMaxRows = (int)((BLOCK_SIZE * 1000) / (sizeof(int) * projCols));
        if (projMaxRows < 1) projMaxRows = 1;

        /* --------------------------------------------------------------
         *  Decide number of hash partitions.
         *
         *  Memory budget during Phase 2:
         *    - 1 page for reading a partition page  (bufferManager)
         *    - 1 page for writing result rows       (bufferManager)
         *    - remaining pages hold the groupMap (working memory)
         *  => rows that can live in working memory =
         *       (MAX_BLOCKS_IN_MEMORY - 2) * projMaxRows
         *
         *  We want each partition to have at most that many rows.
         *  Total rows = srcTable->rowCount (worst-case all rows distinct).
         *
         *  numPartitions = ceil(rowCount / rowsPerPartition)
         *
         *  Memory budget during Phase 1:
         *    - 1 page consumed by the Cursor (source read)
         *    - 1 page buffer per partition (partBufs[p], flushed when full)
         *  => numPartitions <= MAX_BLOCKS_IN_MEMORY - 1
         * ------------------------------------------------------------- */
        int pagesForMap = max(1, (int)MAX_BLOCKS_IN_MEMORY - 2);
        long long rowsPerPartition = (long long)pagesForMap * projMaxRows;
        long long totalRows        = max(1LL, (long long)srcTable->rowCount);

        int numPartitions = (int)((totalRows + rowsPerPartition - 1) / rowsPerPartition);
        if (numPartitions < 1) numPartitions = 1;

        // Phase 1 constraint: 1 Cursor page + 1 page per partition <= MAX_BLOCKS_IN_MEMORY
        int maxPartitions = max(1, (int)MAX_BLOCKS_IN_MEMORY - 1);
        if (numPartitions > maxPartitions) numPartitions = maxPartitions;

        // Helper: map a column name to its accumulator index in the projected
        // schema. "*" maps to the star accumulator.
        int starIdx = (int)colsNeeded[gi].size();
        auto getAccumIdx = [&](const string &colName) -> int {
            if (colName == "*") return starIdx;
            for (int c = 0; c < (int)colsNeeded[gi].size(); c++)
                if (colsNeeded[gi][c].first == colName) return c;
            return -1;
        };

        int havLhsIdx = getAccumIdx(parsedQuery.groupByHavingLHS.columnName);
        int havRhsIdx = parsedQuery.groupByHavingRHSIsAggregate
                            ? getAccumIdx(parsedQuery.groupByHavingRHS.columnName)
                            : -1;
        int retIdx    = getAccumIdx(retAgg.columnName);

        // Create the result table up-front; if it ends empty we unload it.
        Table *resultTable = new Table(resultName, {col1Name, col2Name});
        resultTable->blockCount = 0;
        resultTable->rowCount = 0;
        resultTable->rowsPerBlockCount.clear();

        // Row stream over the SOURCE table using Cursor; yields projected rows.
        Cursor cursor(srcName, 0);
        auto nextFromSource = [&](vector<int> &out) -> bool {
            vector<int> srcRow = cursor.getNext();
            if (srcRow.empty())
                return false;
            out.clear();
            out.reserve((size_t)projCols);
            out.push_back(srcRow[gci]);
            for (auto &[cName, cIdx] : colsNeeded[gi])
                out.push_back(srcRow[cIdx]);
            return true;
        };

        string basePrefix = "GROUPBY_" + srcName + "_G" + to_string(gi);
    processStream(nextFromSource, 0, basePrefix, resultTable,
              projCols, projMaxRows, starIdx, havLhsIdx, havRhsIdx, retIdx, retAgg);

        if (resultTable->rowCount > 0)
            tableCatalogue.insertTable(resultTable);
        else
        {
            resultTable->unload();
            delete resultTable;
        }
    }

    // Clear the buffer pool after the full operation
    bufferManager.clearPool();
}

// Result <- GROUP BY DepartmentID FROM EMP HAVING AVG(Salary) > 30000 RETURN COUNT(*)
// Result1, Result2, Result3 <- GROUP BY DepartmentID, RoleID, YearsOfExperience FROM EMP HAVING AVG(Salary) > AVG(Expenses) RETURN MAX(Salary), MIN(YearsOfExperience), SUM(Expenses)