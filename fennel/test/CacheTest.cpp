/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/CacheTestBase.h"
#include "fennel/test/PagingTestBase.h"
#include "fennel/cache/Cache.h"
#include "fennel/cache/QuotaCacheAccessor.h"
#include "fennel/common/CompoundId.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * CacheTest exercises the entire Cache interface in both
 * single-threaded and multi-threaded modes.  Command-line options
 * allow control over variables such as the test size, duration,
 * and mix of operations.
 *
 *<p>
 *
 * TODO:  doc configuration parameters
 */
class CacheTest : virtual public PagingTestBase
{
public:
    /**
     * BlockIds range from 0 to nDiskPages-1.
     */
    BlockId makeBlockId(uint i)
    {
        assert(i < nDiskPages);
        BlockId blockId;
        CompoundId::setDeviceId(blockId,dataDeviceId);
        CompoundId::setBlockNum(blockId,i);
        return blockId;
    }

    virtual CachePage *lockPage(OpType opType,uint iPage)
    {
        BlockId blockId = makeBlockId(iPage);
        return getCache().lockPage(
            blockId,getLockMode(opType),opType != OP_ALLOCATE);
    }

    virtual void unlockPage(CachePage &page,LockMode lockMode)
    {
        getCache().unlockPage(page,lockMode);
    }

    virtual void prefetchPage(uint iPage)
    {
        BlockId blockId = makeBlockId(iPage);
        getCache().prefetchPage(blockId);
    }
    
    virtual void prefetchBatch(uint iPage,uint nPagesPerBatch)    
    {
        BlockId blockId = makeBlockId(iPage);
        getCache().prefetchBatch(blockId,nPagesPerBatch);
    }
    
    explicit CacheTest()
    {
        // disable irrelevant threads
        threadCounts[OP_ALLOCATE] = 0;
        threadCounts[OP_DEALLOCATE] = 0;

        cbPageUsable = cbPageFull;
        
        FENNEL_UNIT_TEST_CASE(CacheTest,testSingleThread);
        FENNEL_UNIT_TEST_CASE(CacheTest,testQuotaCacheAccessor);
        FENNEL_UNIT_TEST_CASE(PagingTestBase,testMultipleThreads);
    }

    void testSingleThread()
    {
        openStorage(DeviceMode::createNew);
        testAllocateAll();
        testSequentialRead();
        testRandomRead();
        testScratch();
        testSequentialWrite();
        testRandomWrite();
        closeStorage();
        openStorage(DeviceMode::load);
        testRandomRead();
        testSequentialRead();
        closeStorage();
    }

    void testQuotaCacheAccessor()
    {
        openStorage(DeviceMode::createNew);
        QuotaCacheAccessor *pQuota = new QuotaCacheAccessor(
            SharedQuotaCacheAccessor(),pCache,5);
        SharedCacheAccessor pSharedQuota(pQuota);
        BOOST_CHECK_EQUAL(pQuota->getMaxLockedPages(),5U);
        BOOST_CHECK_EQUAL(pQuota->getLockedPageCount(),0U);
        CachePage *pPage = pQuota->lockPage(makeBlockId(0),LOCKMODE_S,0);
        BOOST_CHECK_EQUAL(pQuota->getLockedPageCount(),1U);
        pQuota->unlockPage(*pPage,LOCKMODE_S);
        BOOST_CHECK_EQUAL(pQuota->getLockedPageCount(),0U);
        pSharedQuota.reset();
        closeStorage();
    }
};

FENNEL_UNIT_TEST_SUITE(CacheTest);

// End CacheTest.cpp
