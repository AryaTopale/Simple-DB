#include "../global.h"

/**
 * @brief Executes the SETBUFFER command.
 * Syntax: SETBUFFER K
 * Sets the maximum number of blocks allowed in main memory for this session.
 * Constraint: 2 <= K <= 10 (enforced by semantic parser).
 */
void executeSETBUFFER()
{
    logger.log("executeSETBUFFER");
    if (parsedQuery.setBufferCount < 2 || parsedQuery.setBufferCount > 10)
    {
        cout << "Error: Buffer count must be between 2 and 10." << endl;
        return;
    }
    MAX_BLOCKS_IN_MEMORY = (uint)parsedQuery.setBufferCount;

    // Evict pages exceeding new buffer size
    bufferManager.trimPool();

    cout << "Buffer size set to " << MAX_BLOCKS_IN_MEMORY << endl;
    return;
}
