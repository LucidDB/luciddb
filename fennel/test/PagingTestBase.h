/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef Fennel_PagingTestBase_Included
#define Fennel_PagingTestBase_Included

#include "fennel/test/CacheTestBase.h"
#include "fennel/test/ThreadedTestBase.h"
#include "fennel/synch/SXMutex.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

class CachePage;

/**
 * PagingTestBase is a common base for multi-threaded tests which exercise
 * cache paging.
 */
class PagingTestBase
    : virtual public CacheTestBase,
        virtual public ThreadedTestBase
{
protected:
    /**
     * Portion of each page that should be scribbled on.
     */
    uint cbPageUsable;

    /**
     * Number of random access operations to run per pass.
     */
    uint nRandomOps;

    /**
     * Checkpoint interval during multi-threaded test.
     */
    uint nSecondsBetweenCheckpoints;

    /**
     * Protects output stream.
     */
    StrictMutex logMutex;

    /**
     * SXMutex used to synchronize checkpoints with write actions.
     */
    SXMutex checkpointMutex;
    
    /**
     * Flag indicating that the cache should be dynamically resized
     * during the multi-threaded portion of the test.
     */
    bool bTestResize;

    uint generateRandomNumber(uint iMax);
    
public:
    /**
     * The various operations that can be run in the multi-threaded test.
     */
    enum OpType {
        OP_READ_SEQ,
        OP_WRITE_SEQ,
        OP_READ_RAND,
        OP_WRITE_RAND,
        OP_READ_NOWAIT,
        OP_WRITE_NOWAIT,
        OP_SCRATCH,
        OP_PREFETCH,
        OP_PREFETCH_BATCH,
        OP_ALLOCATE,
        OP_DEALLOCATE,
        OP_CHECKPOINT,
        OP_RESIZE_CACHE,
        OP_MAX
    };

    explicit PagingTestBase();

    virtual ~PagingTestBase();
    
    /**
     * Scribbles on the contents of a page.  The data written is derived from
     * the parameter x.
     */
    virtual void fillPage(CachePage &page,uint x);

    /**
     * Verifies that the page contents are correct (based on the parameter x).
     */
    virtual void verifyPage(CachePage &page,uint x);

    virtual CachePage *lockPage(OpType opType,uint iPage) = 0;
    virtual void unlockPage(CachePage &page,LockMode lockMode) = 0;
    virtual void prefetchPage(uint iPage) = 0;
    virtual void prefetchBatch(uint iPage,uint nPagesPerBatch) = 0;
    
    /**
     * Carries out one operation on a page.  This involves locking the page,
     * calling verifyPage or fillPage, and then unlocking the page.
     *
     * @param opType operation which  will be attempted
     *
     * @param iPage block number of page
     *
     * @param bNice true if page should be marked as nice as part of access
     *
     * @return true if the operation was successfully carried out;
     * false if NoWait locking was requested and the page lock could
     * not be acquired
     */
    bool testOp(OpType opType,uint iPage,bool bNice);

    /**
     * Makes up an operation name based on an OpType.
     */
    char const *getOpName(OpType opType);

    /**
     * Gets the LockMode corresponding to an OpType.
     */
    LockMode getLockMode(OpType opType);
    
    /**
     * Carries out an operation on each disk page in order from
     * 0 to nDiskPages-1.
     *
     * @param opType see testOp
     */
    void testSequentialOp(OpType opType);
    
    /**
     * Carries out an operation on nRandomOps pages selected at random.
     *
     * @param opType see testOp
     */
    void testRandomOp(OpType opType);

    /**
     * Performs nRandomOps scratch operations.  A scratch operation
     * consists of locking a scratch page, filling it with random
     * data, and then unlocking it.
     */
    void testScratch();

    /**
     * Performs a limited number of prefetch operations.  Prefetches
     * are not verified.
     */
    void testPrefetch();

    /**
     * Performs a limited number of batch prefetch operations.  Prefetches
     * are not verified.
     */
    void testPrefetchBatch();

    /**
     * Performs a periodic checkpoint.
     */
    virtual void testCheckpoint();
    
    /**
     * Initializes all disk pages, filling them with information based
     * on their block numbers.
     */
    virtual void testAllocateAll();

    /**
     * Carries out one sequential read pass over the entire device.
     */
    void testSequentialRead();

    /**
     * Carries out one sequential write pass over the entire device.
     */
    void testSequentialWrite();

    /**
     * Carries out nRandomOps read operations on pages selected at random.
     */
    void testRandomRead();

    /**
     * Carries out nRandomOps write operations on pages selected at random.
     */
    void testRandomWrite();

    virtual void testAllocate();
    
    virtual void testDeallocate();

    void testCheckpointGuarded();

    void testCacheResize();
    
    /**
     * Carries out specified tests in multi-threaded mode.
     */
    void testMultipleThreads();
    
    virtual void threadInit();
    
    virtual void threadTerminate();
    
    virtual bool testThreadedOp(int iOp);
};

FENNEL_END_NAMESPACE

#endif

// End PagingTestBase.h
