/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
 * TestCache exercises the entire Cache interface in both
 * single-threaded and multi-threaded modes.  Command-line options
 * allow control over variables such as the test size, duration,
 * and mix of operations.
 *
 *<p>
 *
 * TODO:  doc configuration parameters
 */
class TestCache : virtual public PagingTestBase
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
    
    explicit TestCache()
    {
        // disable irrelevant threads
        threadCounts[OP_ALLOCATE] = 0;
        threadCounts[OP_DEALLOCATE] = 0;

        cbPageUsable = cbPageFull;
        
        FENNEL_UNIT_TEST_CASE(TestCache,testSingleThread);
        FENNEL_UNIT_TEST_CASE(TestCache,testQuotaCacheAccessor);
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
        BOOST_CHECK_EQUAL(pQuota->getMaxLockedPages(),5);
        BOOST_CHECK_EQUAL(pQuota->getLockedPageCount(),0);
        CachePage *pPage = pQuota->lockPage(makeBlockId(0),LOCKMODE_S,0);
        BOOST_CHECK_EQUAL(pQuota->getLockedPageCount(),1);
        pQuota->unlockPage(*pPage,LOCKMODE_S);
        BOOST_CHECK_EQUAL(pQuota->getLockedPageCount(),0);
        pSharedQuota.reset();
        closeStorage();
    }
};

FENNEL_UNIT_TEST_SUITE(TestCache);

// End TestCache.cpp
