#include "global.h"
#include <fstream>
#include <sstream>
#include <map>
#include <set>
#include <vector>
#include <string>
#include <climits>
#include <algorithm>

using namespace std;

// ─────────────────────────────────────────────────────────────────────────────
// Lock types
// ─────────────────────────────────────────────────────────────────────────────
enum LockType { LOCK_NONE, LOCK_SHARED, LOCK_EXCLUSIVE };

struct PageLock {
    LockType      type            = LOCK_NONE;
    set<int>      sharedHolders;          // tx IDs with shared lock
    int           exclusiveHolder = -1;   // tx ID with exclusive lock
};

enum TxStatus { TX_ACTIVE, TX_ABORTED, TX_COMMITTED };

// ─────────────────────────────────────────────────────────────────────────────
// Transaction – uses a vector<pair<>> for locksHeld so that iteration
// produces unlocks in acquisition order (required by sample outputs).
// ─────────────────────────────────────────────────────────────────────────────
struct Transaction {
    int      id        = 0;
    int      timestamp = 0;
    TxStatus status    = TX_ACTIVE;

    // ordered by acquisition time
    vector<pair<string, LockType>> locksHeld;

    LockType getLock(const string& key) const {
        for (auto& p : locksHeld)
            if (p.first == key) return p.second;
        return LOCK_NONE;
    }
    // add new lock or upgrade existing one in place
    void setLock(const string& key, LockType lt) {
        for (auto& p : locksHeld)
            if (p.first == key) { p.second = lt; return; }
        locksHeld.push_back({key, lt});
    }
};

struct Op {
    string opType;      // BEGIN READ WRITE COMMIT
    int    txID    = 0;
    string tableName;
    int    pageNum = 0;
};

// Module-level state (reset on each TRANSACTION call)
static map<string, PageLock>  lockTable;
static map<int, Transaction>  txMap;
static int                    globalTime = 0;

// ─────────────────────────────────────────────────────────────────────────────
// Page key used as lock-table key  (internal only; display uses orig names)
// ─────────────────────────────────────────────────────────────────────────────
static string pageKey(const string& tbl, int pg)
{
    return tbl + " " + to_string(pg);   // e.g. "student 5"
}

static bool startsWith(const string& s, const string& p)
{
    return s.rfind(p, 0) == 0;
}

static bool isPreRestartBlockStart(const string& line)
{
    if (startsWith(line, "COMMIT T") || startsWith(line, "ABORT T") ||
        startsWith(line, "Restart T")) return true;

    if (!line.empty() && line[0] == 'T') {
        if (line.find(" requests ") != string::npos) return true;
        if (line.find(" still waits") != string::npos) return true;
        if ((line.find(" shared lock(") != string::npos ||
             line.find(" exclusive lock(") != string::npos) &&
            line.find(" granted") != string::npos)
            return true;
    }
    return false;
}

static string formatTransactionOutput(const string& raw)
{
    vector<string> lines;
    istringstream in(raw);
    string line;
    while (getline(in, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) continue;
        lines.push_back(line);
    }

    if (lines.empty()) return "";

    string formatted;
    size_t i = 0;

    while (i < lines.size() && startsWith(lines[i], "BEGIN T")) {
        formatted += lines[i++] + "\n";
    }
    if (i > 0 && i < lines.size()) formatted += "\n";

    vector<string> block;
    auto flushBlock = [&]() {
        if (block.empty()) return;
        for (const string& b : block) formatted += b + "\n";
        block.clear();
    };

    // Before first restart, print one blank line after each event block.
    while (i < lines.size() && !startsWith(lines[i], "Restart T")) {
        const string& cur = lines[i++];
        if (isPreRestartBlockStart(cur)) {
            if (!block.empty()) {
                flushBlock();
                formatted += "\n";
            }
            block.push_back(cur);
        } else {
            block.push_back(cur);
        }
    }
    flushBlock();

    // Restart sections should remain contiguous, with one blank line between
    // restart blocks.
    while (i < lines.size()) {
        if (!formatted.empty() && formatted.back() == '\n') formatted += "\n";

        size_t j = i + 1;
        while (j < lines.size() && !startsWith(lines[j], "Restart T")) j++;

        for (size_t k = i; k < j; k++) formatted += lines[k] + "\n";
        i = j;
    }

    return formatted;
}

// ─────────────────────────────────────────────────────────────────────────────
// Release all locks held by a tx (in acquisition order → unlock messages
// appear in the same order the locks were taken).
// ─────────────────────────────────────────────────────────────────────────────
static void releaseAllLocks(int txID, ostream& out)
{
    Transaction& tx = txMap[txID];
    for (auto& [key, lt] : tx.locksHeld) {
        PageLock& pl = lockTable[key];
        if (lt == LOCK_SHARED) {
            pl.sharedHolders.erase(txID);
            if (pl.sharedHolders.empty()) pl.type = LOCK_NONE;
        } else if (lt == LOCK_EXCLUSIVE) {
            pl.exclusiveHolder = -1;
            pl.type = LOCK_NONE;
        }
        out << "unlock(" << key << ") by T" << txID << "\n";
    }
    tx.locksHeld.clear();
}

static void abortTx(int txID, ostream& out)
{
    txMap[txID].status = TX_ABORTED;
    out << "ABORT T" << txID << "\n";
    out << "ROLLBACK T" << txID << "\n";
    releaseAllLocks(txID, out);
}

// ─────────────────────────────────────────────────────────────────────────────
// Wait-die: requester (req) is older (smaller ts) than holder → may wait.
//           requester is younger → must die.
// ─────────────────────────────────────────────────────────────────────────────
static bool requesterIsOlderThan(int req, int holder)
{
    return txMap[req].timestamp < txMap[holder].timestamp;
}

// ─────────────────────────────────────────────────────────────────────────────
// Can txID acquire the given lock NOW (without blocking)?
// ─────────────────────────────────────────────────────────────────────────────
static bool canAcquireNow(int txID, const string& key, bool isWrite)
{
    PageLock& pl = lockTable[key];
    if (pl.type == LOCK_NONE) return true;
    if (isWrite) {
        if (pl.type == LOCK_EXCLUSIVE) return pl.exclusiveHolder == txID;
        // SHARED: upgrade allowed only when this tx is the sole holder
        return (pl.sharedHolders.size() == 1 && pl.sharedHolders.count(txID));
    }
    // READ: compatible with SHARED, or already holds exclusive
    if (pl.type == LOCK_SHARED)    return true;
    return (pl.exclusiveHolder == txID);
}

// ─────────────────────────────────────────────────────────────────────────────
// Acquire shared lock.
// Returns true  = granted.
// Returns false = blocked (waiting) or aborted; sets *aborted accordingly.
// ─────────────────────────────────────────────────────────────────────────────
static bool acquireShared(int txID,
                           const string& tbl, int pg,
                           ostream& out, bool& aborted)
{
    aborted = false;
    string key = pageKey(tbl, pg);
    PageLock& pl  = lockTable[key];
    Transaction& tx = txMap[txID];

    out << "T" << txID << " requests shared lock(" << key << ")\n";

    if (pl.type == LOCK_NONE || pl.type == LOCK_SHARED) {
        pl.type = LOCK_SHARED;
        pl.sharedHolders.insert(txID);
        tx.setLock(key, LOCK_SHARED);
        out << "Lock granted\n";
        return true;
    }
    // Exclusive lock held
    int holder = pl.exclusiveHolder;
    if (holder == txID) { out << "Lock granted\n"; return true; }

    if (requesterIsOlderThan(txID, holder)) {
        out << "T" << txID << " waits (older than T" << holder
            << ", so allowed to wait)\n";
        return false;  // caller adds to waiting
    }
    out << "T" << txID << " dies (younger than T" << holder
        << ", cannot wait)\n";
    abortTx(txID, out);
    aborted = true;
    return false;
}

// ─────────────────────────────────────────────────────────────────────────────
// Acquire exclusive lock.
// ─────────────────────────────────────────────────────────────────────────────
static bool acquireExclusive(int txID,
                              const string& tbl, int pg,
                              ostream& out, bool& aborted)
{
    aborted = false;
    string key = pageKey(tbl, pg);
    PageLock& pl  = lockTable[key];
    Transaction& tx = txMap[txID];

    out << "T" << txID << " requests exclusive lock(" << key << ")\n";

    // No lock → grant
    if (pl.type == LOCK_NONE) {
        pl.type = LOCK_EXCLUSIVE; pl.exclusiveHolder = txID;
        tx.setLock(key, LOCK_EXCLUSIVE);
        out << "Lock granted\n";
        return true;
    }
    // Already holds exclusive
    if (pl.type == LOCK_EXCLUSIVE && pl.exclusiveHolder == txID) {
        out << "Lock granted\n";
        return true;
    }
    // Another tx holds exclusive
    if (pl.type == LOCK_EXCLUSIVE) {
        int holder = pl.exclusiveHolder;
        if (requesterIsOlderThan(txID, holder)) {
            out << "T" << txID << " waits (older than T" << holder
                << ", so allowed to wait)\n";
            return false;
        }
        out << "T" << txID << " dies (younger than T" << holder
            << ", cannot wait)\n";
        abortTx(txID, out); aborted = true;
        return false;
    }
    // SHARED lock(s) held
    if (pl.sharedHolders.size() == 1 && pl.sharedHolders.count(txID)) {
        // Upgrade: this tx is the only shared holder
        pl.sharedHolders.erase(txID);
        pl.type = LOCK_EXCLUSIVE; pl.exclusiveHolder = txID;
        tx.setLock(key, LOCK_EXCLUSIVE);
        out << "Lock granted\n";
        return true;
    }
    // Multiple shared holders – wait-die against each one (except self)
    // Find any holder that is OLDER than requester → requester must die
    int blockerIfDie = -1;
    for (int h : pl.sharedHolders) {
        if (h == txID) continue;
        if (!requesterIsOlderThan(txID, h)) {   // requester is younger than h
            blockerIfDie = h;
            break;
        }
    }
    if (blockerIfDie != -1) {
        out << "T" << txID << " dies (younger than T" << blockerIfDie
            << ", cannot wait)\n";
        abortTx(txID, out); aborted = true;
        return false;
    }
    // Requester is older than ALL holders → wait
    // Report the first blocking holder for the wait message
    int firstBlocker = -1;
    for (int h : pl.sharedHolders) {
        if (h == txID) continue;
        firstBlocker = h;
        break;
    }
    out << "T" << txID << " waits (older than T" << firstBlocker
        << ", so allowed to wait)\n";
    return false;   // caller adds to waiting
}

// Forward decls for mutual recursion
static bool runOp(const Op& op,
                   map<int, Op>& waiting,
                   set<int>& needsRestart,
                   map<int, vector<Op>>& deferred,
                   ostream& out);

static void drainDeferred(int tid,
                           map<int, Op>& waiting,
                           set<int>& needsRestart,
                           map<int, vector<Op>>& deferred,
                           ostream& out);

// ─────────────────────────────────────────────────────────────────────────────
// Attempt to unblock waiting transactions after any lock release.
// ─────────────────────────────────────────────────────────────────────────────
static void processPending(map<int, Op>& waiting,
                            set<int>& needsRestart,
                            map<int, vector<Op>>& deferred,
                            ostream& out)
{
    bool progress = true;
    while (progress) {
        progress = false;
        for (auto it = waiting.begin(); it != waiting.end(); ) {
            int wid   = it->first;
            Op& wop   = it->second;

            if (txMap[wid].status == TX_ABORTED) {
                it = waiting.erase(it); progress = true; continue;
            }

            bool isWrite = (wop.opType == "WRITE");
            string key   = pageKey(wop.tableName, wop.pageNum);
            PageLock& pl = lockTable[key];

            if (canAcquireNow(wid, key, isWrite)) {
                // Grant the lock
                if (isWrite) {
                    pl.sharedHolders.erase(wid);   // in case upgrading
                    pl.type = LOCK_EXCLUSIVE;
                    pl.exclusiveHolder = wid;
                    txMap[wid].setLock(key, LOCK_EXCLUSIVE);
                    out << "T" << wid << " exclusive lock(" << key << ") granted\n";
                } else {
                    if (pl.type == LOCK_NONE) pl.type = LOCK_SHARED;
                    pl.sharedHolders.insert(wid);
                    txMap[wid].setLock(key, LOCK_SHARED);
                    out << "T" << wid << " shared lock(" << key << ") granted\n";
                }
                bufferManager.getPage(wop.tableName, wop.pageNum);
                out << wop.opType << " T" << wid << " "
                    << wop.tableName << " " << wop.pageNum << "\n";
                it = waiting.erase(it); progress = true;
                // Drain any ops that were deferred while this tx was waiting
                drainDeferred(wid, waiting, needsRestart, deferred, out);
                // Iterator may have been invalidated by drainDeferred (which
                // can recursively call processPending). Restart the scan.
                it = waiting.begin();
            } else {
                // Still blocked – check if wait-die forces a die now
                bool mustDie = false; int blocker = -1;
                if (pl.type == LOCK_EXCLUSIVE && pl.exclusiveHolder != wid) {
                    blocker = pl.exclusiveHolder;
                    if (!requesterIsOlderThan(wid, blocker)) mustDie = true;
                } else if (pl.type == LOCK_SHARED) {
                    for (int h : pl.sharedHolders) {
                        if (h == wid) continue;
                        if (!requesterIsOlderThan(wid, h)) {
                            mustDie = true; blocker = h; break;
                        }
                    }
                }
                if (mustDie) {
                    out << "T" << wid << " dies (younger than T" << blocker
                        << ", cannot wait)\n";
                    abortTx(wid, out);
                    needsRestart.insert(wid);
                    deferred.erase(wid);
                    it = waiting.erase(it); progress = true;
                } else {
                    ++it;   // still waiting, no message printed
                }
            }
        }
    }
}

// Drain the deferred queue for a transaction that just unblocked.
static void drainDeferred(int tid,
                           map<int, Op>& waiting,
                           set<int>& needsRestart,
                           map<int, vector<Op>>& deferred,
                           ostream& out)
{
    auto it = deferred.find(tid);
    if (it == deferred.end()) return;
    vector<Op> ops = std::move(it->second);
    deferred.erase(it);

    size_t i = 0;
    while (i < ops.size() && txMap[tid].status == TX_ACTIVE) {
        const Op& dop = ops[i];
        if (dop.opType == "COMMIT") {
            txMap[tid].status = TX_COMMITTED;
            out << "COMMIT T" << tid << "\n";
            releaseAllLocks(tid, out);
            processPending(waiting, needsRestart, deferred, out);
            ++i;
            continue;
        }
        // READ / WRITE
        runOp(dop, waiting, needsRestart, deferred, out);
        if (waiting.count(tid)) {
            // Got blocked again — re-defer the rest
            vector<Op> rest(ops.begin() + i + 1, ops.end());
            if (!rest.empty()) deferred[tid] = std::move(rest);
            return;
        }
        ++i;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Parse one schedule line into an Op.
// ─────────────────────────────────────────────────────────────────────────────
static bool parseLine(const string& rawLine, Op& op)
{
    string line = rawLine;
    if (!line.empty() && line.back() == '\r') line.pop_back();
    if (line.empty() || line[0] == '#') return false;

    istringstream iss(line);
    iss >> op.opType;
    if (op.opType.empty()) return false;

    auto parseTxID = [](const string& s) -> int {
        if (s.size() < 2 || (s[0] != 'T' && s[0] != 't')) return -1;
        try { return stoi(s.substr(1)); } catch (...) { return -1; }
    };

    if (op.opType == "BEGIN" || op.opType == "COMMIT") {
        string txStr; iss >> txStr;
        op.txID = parseTxID(txStr);
        return op.txID >= 0;
    }
    if (op.opType == "READ" || op.opType == "WRITE") {
        string txStr;
        iss >> txStr >> op.tableName >> op.pageNum;
        op.txID = parseTxID(txStr);
        return op.txID >= 0 && !op.tableName.empty();
    }
    return false;
}

// ─────────────────────────────────────────────────────────────────────────────
// Validate that the table and page exist in the catalogue.
// ─────────────────────────────────────────────────────────────────────────────
static bool validateTablePage(const string& tbl, int pg, ostream& out)
{
    if (!tableCatalogue.isTable(tbl)) {
        out << "SEMANTIC ERROR: Relation doesn't exist (" << tbl << ")\n";
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    Table* t = tableCatalogue.getTable(tbl);
    if (pg < 0 || (uint)pg >= t->blockCount) {
        out << "SEMANTIC ERROR: Page doesn't exist (" << tbl
            << " page " << pg << ")\n";
        cout << "SEMANTIC ERROR: Page doesn't exist" << endl;
        return false;
    }
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// Execute one READ/WRITE op.
//   Returns true  = lock granted and op executed.
//   Returns false = blocked (tx added to waiting) or aborted.
// ─────────────────────────────────────────────────────────────────────────────
static bool runOp(const Op& op,
                   map<int, Op>& waiting,
                   set<int>& needsRestart,
                   map<int, vector<Op>>& deferred,
                   ostream& out)
{
    int id = op.txID;
    if (txMap[id].status != TX_ACTIVE) return false;
    if (!validateTablePage(op.tableName, op.pageNum, out)) return false;

    bool aborted = false;
    bool ok      = false;

    if (op.opType == "READ")
        ok = acquireShared(id, op.tableName, op.pageNum, out, aborted);
    else
        ok = acquireExclusive(id, op.tableName, op.pageNum, out, aborted);

    if (ok) {
        bufferManager.getPage(op.tableName, op.pageNum);
        out << op.opType << " T" << id << " "
            << op.tableName << " " << op.pageNum << "\n";
    } else if (aborted) {
        needsRestart.insert(id);
        deferred.erase(id);
        processPending(waiting, needsRestart, deferred, out);
    } else {
        waiting[id] = op;
        processPending(waiting, needsRestart, deferred, out);
    }
    return ok;
}

// ─────────────────────────────────────────────────────────────────────────────
// Replay the original ops of a restarted transaction.
// ─────────────────────────────────────────────────────────────────────────────
static void replayOps(int txID,
                       const vector<Op>& ops,
                       map<int, Op>& waiting,
                       set<int>& needsRestart,
                       map<int, vector<Op>>& deferred,
                       ostream& out)
{
    for (const Op& op : ops) {
        if (op.opType == "BEGIN") continue;
        if (txMap[txID].status == TX_ABORTED) break;

        if (op.opType == "COMMIT") {
            txMap[txID].status = TX_COMMITTED;
            out << "COMMIT T" << txID << "\n";
            releaseAllLocks(txID, out);
            processPending(waiting, needsRestart, deferred, out);
            return;
        }

        // READ or WRITE – attempt once; if blocked, stay in waiting and
        // the outer restart loop will NOT retry (restarted txs should
        // succeed immediately since they have the newest timestamp and
        // all prior txs have either committed or not yet restarted).
        bool done = false;
        while (!done && txMap[txID].status == TX_ACTIVE) {
            done = runOp(op, waiting, needsRestart, deferred, out);
            if (!done && txMap[txID].status == TX_ACTIVE) {
                // Was added to waiting; check if processPending already
                // unblocked us
                if (!waiting.count(txID)) done = true;
            }
        }
        if (txMap[txID].status == TX_ABORTED) break;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main executor entry point
// ─────────────────────────────────────────────────────────────────────────────
void executeTRANSACTION()
{
    logger.log("executeTRANSACTION");

    // Paths
    string inputPath = "../data/" + parsedQuery.transactionFileName;
    string baseName  = parsedQuery.transactionFileName;
    size_t dot = baseName.rfind('.');
    if (dot != string::npos) baseName = baseName.substr(0, dot);
    string outputPath = "../data/" + baseName + "_output.txt";

    ifstream fin(inputPath);
    if (!fin.is_open()) {
        cout << "ERROR: Cannot open transaction file: " << inputPath << endl;
        return;
    }
    ofstream outFile(outputPath);
    if (!outFile.is_open()) {
        cout << "ERROR: Cannot create output file: " << outputPath << endl;
        return;
    }
    ostringstream out;

    // ── Parse schedule ─────────────────────────────────────────────────────
    vector<Op> schedule;
    string line;
    while (getline(fin, line)) {
        Op op;
        if (parseLine(line, op)) schedule.push_back(op);
    }
    fin.close();

    // ── Reset global state ─────────────────────────────────────────────────
    lockTable.clear();
    txMap.clear();
    globalTime = 0;

    // Collect original ops per tx (for restart replay)
    map<int, vector<Op>> originalOps;
    for (auto& op : schedule)
        originalOps[op.txID].push_back(op);

    map<int, Op>          waiting;       // txID → currently-blocked op
    set<int>              needsRestart;  // aborted txIDs that must restart
    map<int, vector<Op>>  deferred;      // ops queued while tx was waiting

    // ── Main schedule loop ─────────────────────────────────────────────────
    for (auto& op : schedule) {
        globalTime++;

        // Skip ops for aborted txs (will be replayed on restart)
        if (txMap.count(op.txID) &&
            txMap[op.txID].status == TX_ABORTED) continue;
        // If tx is currently waiting (or already has deferred ops queued),
        // append this op to its deferred queue rather than dropping it.
        if (waiting.count(op.txID) || deferred.count(op.txID)) {
            deferred[op.txID].push_back(op);
            continue;
        }

        if (op.opType == "BEGIN") {
            Transaction tx;
            tx.id = op.txID; tx.timestamp = globalTime;
            txMap[op.txID] = tx;
            out << "BEGIN T" << op.txID << "\n";
            continue;
        }

        if (op.opType == "COMMIT") {
            if (!txMap.count(op.txID)) continue;
            Transaction& tx = txMap[op.txID];
            if (tx.status != TX_ACTIVE) continue;
            tx.status = TX_COMMITTED;
            out << "COMMIT T" << op.txID << "\n";
            releaseAllLocks(op.txID, out);
            processPending(waiting, needsRestart, deferred, out);
            continue;
        }

        // READ or WRITE
        runOp(op, waiting, needsRestart, deferred, out);
    }

    // ── Restart aborted transactions in original-timestamp order ───────────
    while (!needsRestart.empty()) {
        // Pick the tx with the smallest original timestamp
        int target = *min_element(
            needsRestart.begin(), needsRestart.end(),
            [](int a, int b) {
                return txMap[a].timestamp < txMap[b].timestamp;
            });
        needsRestart.erase(target);

        globalTime++;
        out << "Restart T" << target << "\n";

        // Recreate with a new (higher) timestamp
        Transaction tx;
        tx.id = target; tx.timestamp = globalTime;
        txMap[target] = tx;

        replayOps(target, originalOps[target], waiting, needsRestart, deferred, out);
    }

    outFile << formatTransactionOutput(out.str());
    outFile.close();
    cout << "Transaction schedule executed. Output: " << outputPath << endl;
}
