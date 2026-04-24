#include "global.h"

BufferManager::BufferManager()
{
    logger.log("BufferManager::BufferManager");
    
}

/**
 * @brief Function called to read a page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param tableName 
 * @param pageIndex 
 * @return Page 
 */
Page BufferManager::getPage(string tableName, int pageIndex)
{
    logger.log("BufferManager::getPage");
    string pageName = "../data/temp/"+tableName + "_Page" + to_string(pageIndex);
    if (this->inPool(pageName))
        return this->getFromPool(pageName);
    else{
        BLOCK_ACCESSES++;
        // cout << "MISS: " << pageName << endl;
        return this->insertIntoPool(tableName, pageIndex);
}}

/**
 * @brief Checks to see if a page exists in the pool
 *
 * @param pageName 
 * @return true 
 * @return false 
 */
bool BufferManager::inPool(string pageName)
{
    logger.log("BufferManager::inPool");
    for (auto page : this->pages)
    {
        if (pageName == page.pageName)
            return true;
    }
    return false;
}

/**
 * @brief If the page is present in the pool, then this function returns the
 * page. Note that this function will fail if the page is not present in the
 * pool.
 *
 * @param pageName 
 * @return Page 
 */
Page BufferManager::getFromPool(string pageName)
{
    logger.log("BufferManager::getFromPool");
    for (auto page : this->pages)
        if (pageName == page.pageName)
            return page;
}

/**
 * @brief Inserts page indicated by tableName and pageIndex into pool. If the
 * pool is full, the pool ejects the oldest inserted page from the pool and adds
 * the current page at the end. It naturally follows a queue data structure. 
 *
 * @param tableName 
 * @param pageIndex 
 * @return Page 
 */
Page BufferManager::insertIntoPool(string tableName, int pageIndex)
{
    logger.log("BufferManager::insertIntoPool");
    Page page(tableName, pageIndex);
    if (this->pages.size() >= MAX_BLOCKS_IN_MEMORY){
        pages.pop_front();
    }
    pages.push_back(page);
    return page;
}

/**
 * @brief The buffer manager is also responsible for writing pages. This is
 * called when new tables are created using assignment statements.
 *
 * @param tableName 
 * @param pageIndex 
 * @param rows 
 * @param rowCount 
 */
void BufferManager::writePage(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("BufferManager::writePage");
    Page page(tableName, pageIndex, rows, rowCount);
    page.writePage();
    // Invalidate any stale copy of this page in the pool
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    for (auto it = this->pages.begin(); it != this->pages.end(); ++it){
        if (it->pageName == pageName){
            this->pages.erase(it);
            break;
        }
    }
}

/**
 * @brief Read a temporary page that is NOT registered in tableCatalogue.
 *        Uses the buffer pool with FIFO replacement, just like getPage().
 *
 * @param pageName    full file path, e.g. "../data/temp/SORT_R_R0_Page0"
 * @param columnCount number of int columns per row
 * @param rowCount    number of rows in the file
 * @return Page
 */
Page BufferManager::getTempPage(string pageName, int columnCount, int rowCount)
{
    logger.log("BufferManager::getTempPage");
    if (this->inPool(pageName))
        return this->getFromPool(pageName);
    // Not in pool – read from disk, insert into pool
    BLOCK_ACCESSES++;
    Page page(pageName, columnCount, rowCount);
    if (this->pages.size() >= MAX_BLOCKS_IN_MEMORY){
        pages.pop_front();
    }
    pages.push_back(page);
    return page;
}

/**
 * @brief Write a temporary page that is NOT registered in tableCatalogue.
 *        Constructs a Page with the given pageName and rows and writes to disk.
 *        Also invalidates any stale copy in the pool.
 *
 * @param pageName    full file path
 * @param rows        row data
 * @param rowCount    number of rows
 */
void BufferManager::writeTempPage(string pageName, vector<vector<int>> rows, int rowCount)
{
    logger.log("BufferManager::writeTempPage");
    // Remove stale copy from pool if present
    for (auto it = this->pages.begin(); it != this->pages.end(); ++it){
        if (it->pageName == pageName){
            this->pages.erase(it);
            break;
        }
    }
    // Write to disk using the temp-page Page constructor then writePage()
    Page page(pageName, rows[0].size(), 0); // create with 0 rows (we'll overwrite)
    // Actually just write directly – simpler and correct
    ofstream fout(pageName, ios::trunc);
    for (int r = 0; r < rowCount; r++){
        for (int c = 0; c < (int)rows[r].size(); c++){
            if (c) fout << " ";
            fout << rows[r][c];
        }
        fout << endl;
    }
    fout.close();
}

/**
 * @brief Deletes file names fileName
 *
 * @param fileName 
 */
void BufferManager::deleteFile(string fileName)
{
    
    if (remove(fileName.c_str())){
        cout << "Error deleting file: " << fileName << endl;
        logger.log("BufferManager::deleteFile: Err");}
        else logger.log("BufferManager::deleteFile: Success");
}

/**
 * @brief Overloaded function that calls deleteFile(fileName) by constructing
 * the fileName from the tableName and pageIndex.
 *
 * @param tableName 
 * @param pageIndex 
 */
void BufferManager::deleteFile(string tableName, int pageIndex)
{
    logger.log("BufferManager::deleteFile");
    string fileName = "../data/temp/"+tableName + "_Page" + to_string(pageIndex);
    this->deleteFile(fileName);
}


Page BufferManager::getPageByFileName(const string &pageName)
{
    logger.log("BufferManager::getPageByFileName");
    // If in pool, return; else construct a Page by parsing pageName to infer tableName/pageIndex
    // Minimal implementation: use a Page ctor that matches your needs or read file directly.
    // TODO: implement robust parsing or a direct-page reader
    return Page(); 
}

void BufferManager::writePageByFileName(const string &pageName, vector<vector<int>> rows, int rowCount)
{
    logger.log("BufferManager::writePageByFileName");
    ofstream fout(pageName, ios::trunc);
    for (int r = 0; r < rowCount; ++r) {
        for (int c = 0; c < rows[r].size(); ++c) {
            if (c) fout << " ";
            fout << rows[r][c];
        }
        fout << "\n";
    }
    fout.close();
}

void BufferManager::deleteFileByFileName(const string &fileName)
{
    logger.log("BufferManager::deleteFileByFileName");
    if (remove(fileName.c_str()))
        logger.log("BufferManager::deleteFileByFileName: Err");
    else
        logger.log("BufferManager::deleteFileByFileName: Success");
}

void BufferManager::trimPool()
{
    logger.log("BufferManager::trimPool");
    while (this->pages.size() > MAX_BLOCKS_IN_MEMORY)
    {
        this->pages.pop_front();
    }
}

void BufferManager::clearPool()
{
    logger.log("BufferManager::clearPool");
    this->pages.clear();
}