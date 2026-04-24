#include "global.h"
#include <queue>
/**
 * @brief File contains method to process SORT commands.
 *
 * syntax:
 * SORT <table-name> BY <col1>, <col2>, ... IN <ASC|DESC>, <ASC|DESC>, ...
 *   [TOP X] [BOTTOM Y]
 */

bool syntacticParseSORT()
{
    logger.log("syntacticParseSORT");

    // Minimum: SORT table BY col IN order => 5 tokens
    if (tokenizedQuery.size() < 5)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = SORT;
    parsedQuery.sortRelationName = tokenizedQuery[1];

    if (tokenizedQuery[2] != "BY")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Parse column names after BY until we hit IN
    int idx = 3;
    vector<string> colNames;
    while (idx < (int)tokenizedQuery.size() && tokenizedQuery[idx] != "IN")
    {
        colNames.push_back(tokenizedQuery[idx]);
        idx++;
    }

    if (colNames.empty() || idx >= (int)tokenizedQuery.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // tokenizedQuery[idx] == "IN"
    idx++; // skip "IN"

    // Parse sorting strategies
    vector<string> strategies;
    while (idx < (int)tokenizedQuery.size() && tokenizedQuery[idx] != "TOP" && tokenizedQuery[idx] != "BOTTOM")
    {
        strategies.push_back(tokenizedQuery[idx]);
        idx++;
    }

    if (colNames.size() != strategies.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Validate strategies
    for (auto &s : strategies)
    {
        if (s != "ASC" && s != "DESC")
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }

    parsedQuery.sortColumnNames = colNames;
    for (auto &s : strategies)
    {
        if (s == "ASC")
            parsedQuery.sortingStrategies.push_back(ASC);
        else
            parsedQuery.sortingStrategies.push_back(DESC);
    }

    // Parse optional TOP X and BOTTOM Y
    while (idx < (int)tokenizedQuery.size())
    {
        if (tokenizedQuery[idx] == "TOP")
        {
            idx++;
            if (idx >= (int)tokenizedQuery.size())
            {
                cout << "SYNTAX ERROR" << endl;
                return false;
            }
            try
            {
                parsedQuery.sortTopRows = stoi(tokenizedQuery[idx]);
            }
            catch (...)
            {
                cout << "SYNTAX ERROR" << endl;
                return false;
            }
            if (parsedQuery.sortTopRows <= 0)
            {
                cout << "SYNTAX ERROR" << endl;
                return false;
            }
            idx++;
        }
        else if (tokenizedQuery[idx] == "BOTTOM")
        {
            idx++;
            if (idx >= (int)tokenizedQuery.size())
            {
                cout << "SYNTAX ERROR" << endl;
                return false;
            }
            try
            {
                parsedQuery.sortBottomRows = stoi(tokenizedQuery[idx]);
            }
            catch (...)
            {
                cout << "SYNTAX ERROR" << endl;
                return false;
            }
            if (parsedQuery.sortBottomRows <= 0)
            {
                cout << "SYNTAX ERROR" << endl;
                return false;
            }
            idx++;
        }
        else
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }

    return true;
}

bool semanticParseSORT()
{
    logger.log("semanticParseSORT");

    if (!tableCatalogue.isTable(parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    for (auto &colName : parsedQuery.sortColumnNames)
    {
        if (!tableCatalogue.isColumnFromTable(colName, parsedQuery.sortRelationName))
        {
            cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
            return false;
        }
    }

    return true;
}

/* ------------------------------------------------------------------ */
/*  Helpers for reading / writing temp sort-run pages via              */
/*  bufferManager.getTempPage / bufferManager.writeTempPage            */
/* ------------------------------------------------------------------ */

/**
 * @brief Read all rows from a temporary sort-run page through the buffer manager.
 */
static vector<vector<int>> readTempRunPage(const string &fileName, int columnCount, int rowCount)
{
    Page page = bufferManager.getTempPage(fileName, columnCount, rowCount);
    vector<vector<int>> rows;
    rows.reserve(rowCount);
    for (int r = 0; r < rowCount; r++)
    {
        rows.push_back(page.getRow(r));
    }
    return rows;
}

/**
 * @brief Write rows to a temporary sort-run page through the buffer manager.
 */
static void writeTempRunPage(const string &fileName, vector<vector<int>> &rows, int rowCount)
{
    bufferManager.writeTempPage(fileName, rows, rowCount);
}

/* ------------------------------------------------------------------ */
/*  Helpers for reading / writing the ORIGINAL table pages via        */
/*  bufferManager.getPage  and  bufferManager.writePage               */
/* ------------------------------------------------------------------ */

/**
 * @brief Read all rows of a table page through the BufferManager / Page API.
 */
static vector<vector<int>> readTablePage(const string &tableName, int pageIndex, int rowCount)
{
    Page page = bufferManager.getPage(tableName, pageIndex);
    vector<vector<int>> rows;
    rows.reserve(rowCount);
    for (int r = 0; r < rowCount; r++)
    {
        rows.push_back(page.getRow(r));
    }
    return rows;
}

/**
 * @brief Write all rows of a table page through the BufferManager API.
 */
static void writeTablePage(const string &tableName, int pageIndex,
                           vector<vector<int>> &rows, int rowCount)
{
    bufferManager.writePage(tableName, pageIndex, rows, rowCount);
}

/* ------------------------------------------------------------------ */
/*  Multi-column comparison with stability                            */
/* ------------------------------------------------------------------ */

struct RowWithIndex
{
    vector<int> row;
    long long originalIndex; // global row index for stability
};

/* ------------------------------------------------------------------ */
/*  Custom stable sort (no std::sort)                                 */
/* ------------------------------------------------------------------ */

template <typename T, typename Compare>
static void mergeInto(vector<T> &arr, vector<T> &buffer, int left, int mid, int right, Compare cmp)
{
    int i = left;
    int j = mid;
    int k = left;

    // Stable merge: if equal (neither a<b nor b<a), take from left side first.
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

static bool compareRows(const RowWithIndex &a, const RowWithIndex &b,
                        const vector<int> &colIndices,
                        const vector<SortingStrategy> &strategies)
{
    for (int i = 0; i < (int)colIndices.size(); i++)
    {
        int ci = colIndices[i];
        if (a.row[ci] != b.row[ci])
        {
            if (strategies[i] == ASC)
                return a.row[ci] < b.row[ci];
            else
                return a.row[ci] > b.row[ci];
        }
    }
    // All sort keys equal: preserve original order
    return a.originalIndex < b.originalIndex;
}

/* ------------------------------------------------------------------ */
/*  K-way Multi-Phase External Merge Sort                             */
/* ------------------------------------------------------------------ */

/**
 * @brief Perform K-way external merge sort on a range of rows.
 *        The range is [startRow, startRow + numRows) across the table's pages.
 *
 *  Phase 0 – create sorted runs of K pages each (read K pages via
 *            bufferManager.getPage, sort in memory, write to temp run files).
 *  Phase 1+ – (K-1)-way merge until a single run remains.
 *  Final   – read the single sorted run and write back into the original
 *            table pages via bufferManager.writePage.
 */
static void externalMergeSort(Table *table, const vector<int> &colIndices,
                              const vector<SortingStrategy> &strategies,
                              long long startRow, long long numRows)
{
    if (numRows <= 0)
        return;

    string tableName = table->tableName;
    int columnCount = table->columnCount;
    int maxRowsPerBlock = table->maxRowsPerBlock;
    int totalBlocks = table->blockCount;
    int K = MAX_BLOCKS_IN_MEMORY;

    // Build prefix sum of rows-per-block
    vector<long long> prefixRows(totalBlocks + 1, 0);
    for (int i = 0; i < totalBlocks; i++)
    {
        prefixRows[i + 1] = prefixRows[i] + table->rowsPerBlockCount[i];
    }

    long long endRow = startRow + numRows; // exclusive

    // Identify first/last page that overlaps our row range
    int firstPage = -1, lastPage = -1;
    for (int i = 0; i < totalBlocks; i++)
    {
        if (prefixRows[i + 1] > startRow && prefixRows[i] < endRow)
        {
            if (firstPage == -1)
                firstPage = i;
            lastPage = i;
        }
    }
    if (firstPage == -1)
        return;

    /* ============================================================
     *  Phase 0 – Create initial sorted runs (each up to K pages)
     * ============================================================ */

    /* ============================================================
     *  Phase 0 – Create initial sorted runs (K pages at a time)
     *  WITHOUT loading all rows into memory at once
     * ============================================================ */

    string sortPrefix = "SORT_" + tableName;
    long long runSizeRows = (long long)K * maxRowsPerBlock;
    int numRuns = 0;
    vector<vector<int>> runRowsPerPage;

    // Iterate over pages in the target range, K pages at a time
    for (int p = firstPage; p <= lastPage;)
    {
        // Collect up to K pages worth of rows into a temporary buffer
        vector<RowWithIndex> chunk;
        long long localIdx = 0;
        int pagesRead = 0;

        while (p <= lastPage && pagesRead < K)
        {
            // Read ONE page via bufferManager (only K pages ever in memory)
            Page page = bufferManager.getPage(tableName, p);
            int rowsInPage = table->rowsPerBlockCount[p];

            for (int r = 0; r < rowsInPage; r++)
            {
                long long globalRowIdx = prefixRows[p] + r;
                if (globalRowIdx >= startRow && globalRowIdx < endRow)
                {
                    chunk.push_back({page.getRow(r), localIdx++});
                }
            }
            p++;
            pagesRead++;
        }

        if (chunk.empty())
            continue;

        // Sort the chunk in memory
        auto cmp = [&](const RowWithIndex &a, const RowWithIndex &b)
        {
            return compareRows(a, b, colIndices, strategies);
        };
        stableMergeSort(chunk, cmp);

        // Write sorted run to temp pages
        int pagesInRun = 0;
        vector<int> rowsPerPageInRun;

        for (int i = 0; i < (int)chunk.size();)
        {
            int pageEnd = min(i + maxRowsPerBlock, (int)chunk.size());
            int rowsInPage = pageEnd - i;

            vector<vector<int>> pageData;
            for (int j = i; j < pageEnd; j++)
            {
                pageData.push_back(chunk[j].row);
            }

            string pageName = "../data/temp/" + sortPrefix + "_R" + to_string(numRuns) + "_Page" + to_string(pagesInRun);
            writeTempRunPage(pageName, pageData, rowsInPage);
            rowsPerPageInRun.push_back(rowsInPage);
            pagesInRun++;
            i = pageEnd;
        }

        runRowsPerPage.push_back(rowsPerPageInRun);
        numRuns++;
    }

    // Free the in-memory copy
    // allRows.clear();
    // allRows.shrink_to_fit();

    // Free the in-memory copy
    
    /* ============================================================
     *  Merge phases – (K-1)-way merge until a single run remains
     * ============================================================ */
    int mergeWays = max(K - 1, 2);

    while (numRuns > 1)
    {
        int newNumRuns = 0;
        vector<vector<int>> newRunRowsPerPage;

        for (int r = 0; r < numRuns; r += mergeWays)
        {
            int runsToMerge = min(mergeWays, numRuns - r);

            if (runsToMerge == 1)
            {
                // Single remaining run – just rename files to new-run namespace
                int srcRun = r;
                int dstRun = newNumRuns;
                int pagesInRun = runRowsPerPage[srcRun].size();
                vector<int> newRowsPP;
                for (int p = 0; p < pagesInRun; p++)
                {
                    string srcName = "../data/temp/" + sortPrefix + "_R" + to_string(srcRun) + "_Page" + to_string(p);
                    string dstName = "../data/temp/" + sortPrefix + "_NR" + to_string(dstRun) + "_Page" + to_string(p);
                    rename(srcName.c_str(), dstName.c_str());
                    newRowsPP.push_back(runRowsPerPage[srcRun][p]);
                }
                newRunRowsPerPage.push_back(newRowsPP);
                newNumRuns++;
                continue;
            }

            /* -- set up a cursor-like struct for each input run -- */
            struct RunCursor
            {
                int runIdx;
                int pageInRun;
                int rowInPage;
                int totalPages;
                vector<vector<int>> pageData;
                int pageRows;
                bool exhausted;
            };

            vector<RunCursor> cursors(runsToMerge);
            for (int i = 0; i < runsToMerge; i++)
            {
                int ri = r + i;
                cursors[i].runIdx = ri;
                cursors[i].pageInRun = 0;
                cursors[i].rowInPage = 0;
                cursors[i].totalPages = runRowsPerPage[ri].size();
                cursors[i].exhausted = false;

                // Load first page of this run
                string pageName = "../data/temp/" + sortPrefix + "_R" + to_string(ri) + "_Page0";
                int rowsInPage = runRowsPerPage[ri][0];
                cursors[i].pageData = readTempRunPage(pageName, columnCount, rowsInPage);
                cursors[i].pageRows = rowsInPage;
            }

            // Min-heap comparing the current row of each run cursor
            auto pqCmp = [&](int a, int b) -> bool
            {
                const auto &rowA = cursors[a].pageData[cursors[a].rowInPage];
                const auto &rowB = cursors[b].pageData[cursors[b].rowInPage];
                for (int i = 0; i < (int)colIndices.size(); i++)
                {
                    int ci = colIndices[i];
                    if (rowA[ci] != rowB[ci])
                    {
                        if (strategies[i] == ASC)
                            return rowA[ci] > rowB[ci]; // min-heap
                        else
                            return rowA[ci] < rowB[ci];
                    }
                }
                // Stability: earlier run index wins
                return a > b;
            };

            priority_queue<int, vector<int>, decltype(pqCmp)> pq(pqCmp);
            for (int i = 0; i < runsToMerge; i++)
            {
                if (!cursors[i].exhausted)
                    pq.push(i);
            }

            // Output buffer (1 page)
            vector<vector<int>> outputBuffer;
            int outputPageIdx = 0;
            vector<int> newRowsPP;
            int dstRun = newNumRuns;

            auto flushOutput = [&]()
            {
                if (outputBuffer.empty())
                    return;
                string pageName = "../data/temp/" + sortPrefix + "_NR" + to_string(dstRun) + "_Page" + to_string(outputPageIdx);
                writeTempRunPage(pageName, outputBuffer, (int)outputBuffer.size());
                newRowsPP.push_back((int)outputBuffer.size());
                outputPageIdx++;
                outputBuffer.clear();
            };

            while (!pq.empty())
            {
                int ci = pq.top();
                pq.pop();
                outputBuffer.push_back(cursors[ci].pageData[cursors[ci].rowInPage]);
                cursors[ci].rowInPage++;

                // If current page exhausted, load next page of that run
                if (cursors[ci].rowInPage >= cursors[ci].pageRows)
                {
                    cursors[ci].pageInRun++;
                    if (cursors[ci].pageInRun >= cursors[ci].totalPages)
                    {
                        cursors[ci].exhausted = true;
                    }
                    else
                    {
                        string pageName = "../data/temp/" + sortPrefix + "_R" + to_string(cursors[ci].runIdx) + "_Page" + to_string(cursors[ci].pageInRun);
                        int rowsInPage = runRowsPerPage[cursors[ci].runIdx][cursors[ci].pageInRun];
                        cursors[ci].pageData = readTempRunPage(pageName, columnCount, rowsInPage);
                        cursors[ci].pageRows = rowsInPage;
                        cursors[ci].rowInPage = 0;
                    }
                }

                if (!cursors[ci].exhausted)
                    pq.push(ci);

                if ((int)outputBuffer.size() >= maxRowsPerBlock)
                    flushOutput();
            }
            flushOutput();

            newRunRowsPerPage.push_back(newRowsPP);
            newNumRuns++;
        }

        // Delete old run files, rename new run files -> current
        // Clear pool first so stale temp pages are evicted
        bufferManager.clearPool();
        for (int ri = 0; ri < numRuns; ri++)
        {
            for (int p = 0; p < (int)runRowsPerPage[ri].size(); p++)
            {
                string fname = "../data/temp/" + sortPrefix + "_R" + to_string(ri) + "_Page" + to_string(p);
                bufferManager.deleteFile(fname);
            }
        }
        for (int ri = 0; ri < newNumRuns; ri++)
        {
            for (int p = 0; p < (int)newRunRowsPerPage[ri].size(); p++)
            {
                string src = "../data/temp/" + sortPrefix + "_NR" + to_string(ri) + "_Page" + to_string(p);
                string dst = "../data/temp/" + sortPrefix + "_R" + to_string(ri) + "_Page" + to_string(p);
                rename(src.c_str(), dst.c_str());
            }
        }

        numRuns = newNumRuns;
        runRowsPerPage = newRunRowsPerPage;
    }

    /* ============================================================
     *  Write the single sorted run back into the original table
     *  pages using bufferManager.writePage
     * ============================================================ */

    // Clear pool so we read fresh data for both sorted run and original pages
    bufferManager.clearPool();

    // Set up a cursor-like reader over the sorted run
    int sortedPageIdx = 0;
    int sortedRowIdx = 0;
    int sortedPageRows = runRowsPerPage[0][0];
    string sortedFileName = "../data/temp/" + sortPrefix + "_R0_Page0";
    vector<vector<int>> sortedPageData = readTempRunPage(sortedFileName, columnCount, sortedPageRows);

    auto getNextSortedRow = [&]() -> vector<int>
    {
        if (sortedRowIdx >= sortedPageRows)
        {
            sortedPageIdx++;
            if (sortedPageIdx >= (int)runRowsPerPage[0].size())
                return {};
            sortedPageRows = runRowsPerPage[0][sortedPageIdx];
            sortedFileName = "../data/temp/" + sortPrefix + "_R0_Page" + to_string(sortedPageIdx);
            sortedPageData = readTempRunPage(sortedFileName, columnCount, sortedPageRows);
            sortedRowIdx = 0;
        }
        return sortedPageData[sortedRowIdx++];
    };

    // Write sorted rows back into the original table pages
    for (int p = firstPage; p <= lastPage; p++)
    {
        // --- read original page via bufferManager ---
        Page page = bufferManager.getPage(tableName, p);
        int rowsInPage = table->rowsPerBlockCount[p];
        vector<vector<int>> pageRows;
        pageRows.reserve(rowsInPage);
        for (int r = 0; r < rowsInPage; r++)
        {
            pageRows.push_back(page.getRow(r));
        }

        bool modified = false;
        for (int r = 0; r < rowsInPage; r++)
        {
            long long globalRowIdx = prefixRows[p] + r;
            if (globalRowIdx >= startRow && globalRowIdx < endRow)
            {
                vector<int> sortedRow = getNextSortedRow();
                if (!sortedRow.empty())
                {
                    pageRows[r] = sortedRow;
                    modified = true;
                }
            }
        }

        if (modified)
        {
            // --- write modified page via bufferManager ---
            writeTablePage(tableName, p, pageRows, rowsInPage);
        }
    }

    // Clean up temp sort run files
    for (int p = 0; p < (int)runRowsPerPage[0].size(); p++)
    {
        string fname = "../data/temp/" + sortPrefix + "_R0_Page" + to_string(p);
        bufferManager.deleteFile(fname);
    }
}

/* ------------------------------------------------------------------ */
/*  Execute SORT                                                       */
/* ------------------------------------------------------------------ */

void executeSORT()
{
    logger.log("executeSORT");

    Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);

    // Always sort from the original CSV on disk, not from previously sorted
    // temp pages.  Re-load: delete old temp pages, reset metadata, re-blockify.
    bufferManager.clearPool();
    for (uint i = 0; i < table->blockCount; i++)
    {
        bufferManager.deleteFile(table->tableName, i);
    }
    table->columns.clear();
    table->distinctValuesPerColumnCount.clear();
    table->rowsPerBlockCount.clear();
    table->columnCount = 0;
    table->rowCount = 0;
    table->blockCount = 0;
    table->maxRowsPerBlock = 0;
    table->load();

    // Build column-index list from column names
    vector<int> colIndices;
    for (auto &colName : parsedQuery.sortColumnNames)
    {
        colIndices.push_back(table->getColumnIndex(colName));
    }

    // Clear the buffer pool to ensure we read fresh data from disk
    bufferManager.clearPool();

    long long totalRows = table->rowCount;
    int topRows = parsedQuery.sortTopRows;
    int bottomRows = parsedQuery.sortBottomRows;

    if (topRows == -1 && bottomRows == -1)
    {
        // Sort entire table
        externalMergeSort(table, colIndices, parsedQuery.sortingStrategies, 0, totalRows);
    }
    else
    {
        if (topRows != -1)
        {
            long long n = min((long long)topRows, totalRows);
            externalMergeSort(table, colIndices, parsedQuery.sortingStrategies, 0, n);
        }
        // Clear buffer pool between the two independent sorts
        bufferManager.clearPool();
        if (bottomRows != -1)
        {
            long long n = min((long long)bottomRows, totalRows);
            long long startRow = totalRows - n;
            externalMergeSort(table, colIndices, parsedQuery.sortingStrategies, startRow, n);
        }
    }

    // Clear buffer pool after sort so subsequent reads get fresh data
    bufferManager.clearPool();
    return;
}