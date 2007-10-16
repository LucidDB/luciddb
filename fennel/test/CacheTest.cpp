/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifdef HAVE_MMAP
#include <sys/resource.h>
#endif

using namespace fennel;

#define SMALL_ADDR_SPACE (192 * 1024 * 1024) // 192 MB

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
        BlockId blockId(0);
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

#ifdef RLIMIT_AS
        FENNEL_UNIT_TEST_CASE(CacheTest,testLargeCacheInit);
        FENNEL_UNIT_TEST_CASE(CacheTest,testLargeCacheRequest);
#endif
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

#ifdef RLIMIT_AS
    // Sets max address space to minLimit if lower than the current limit.
    // Returns the limit in effect upon return.
    rlim_t setAddressSpaceLimit(rlim_t minLimit, struct rlimit &oldLimits)
    {
        int rv = getrlimit(RLIMIT_AS, &oldLimits);
        BOOST_REQUIRE(rv == 0);

        // Modify them, if necessary
        if (oldLimits.rlim_cur > minLimit) {
            // Lower current "soft" limit
            struct rlimit new_limits;
            new_limits.rlim_cur = minLimit;
            new_limits.rlim_max = oldLimits.rlim_max;

            rv = setrlimit(RLIMIT_AS, &new_limits);
            BOOST_REQUIRE(rv == 0);

            return minLimit;
        }

        return oldLimits.rlim_cur;
    }

    // Resets limit to values returned by reference in setAddressSpaceLimit
    void restoreAddressSpaceLimit(const struct rlimit &limits)
    {
        // restore addres space limits
        int rv = setrlimit(RLIMIT_AS, &limits);
        BOOST_REQUIRE(rv == 0);
    }

    uint computeMaxPagesUpperBound(uint addressSpaceSize, uint pageSize)
    {
        int guardPages = 0;
#ifndef NDEBUG
        // In debug mode, guard pages flank each real page.
        guardPages = 2;
#endif
        int osPageSize = getpagesize();

        return addressSpaceSize / (pageSize + (guardPages * osPageSize) + 4);
    }

    void testLargeCacheInit()
    {
        struct rlimit savedLimits;

        rlim_t addrSpaceSize = setAddressSpaceLimit(
            SMALL_ADDR_SPACE, savedLimits);

        CacheParams params;

        // Ask for so many max pages, that CacheImpl cannot keep track of them
        // all (e.g., vector of page pointers is larger than address space
        // limit)
        params.nMemPagesMax = addrSpaceSize / sizeof(void *) + 100;
        params.nMemPagesInit = 10;

        SharedCache pCache;

        BOOST_CHECK_NO_THROW(pCache = Cache::newCache(params));
        BOOST_CHECK_EQUAL(
            CacheParams::defaultMemPagesMax,
            pCache->getMaxAllocatedPageCount());

        // Note: use of CacheParams::defaultMemPagesMax is correct here.  When
        // we reset the max pages value due to an exception we automatically
        // reset the init value to the default (MAXU), which causes the
        // allocated page count to be equal to the MAX.
        BOOST_CHECK_EQUAL(
            CacheParams::defaultMemPagesMax,
            pCache->getAllocatedPageCount());

        pCache.reset();

        // Set max pages such that allocating that many pages would exceed the
        // process address space.  Try to initialize with that many pages.

        // REVIEW: SWZ: 10/11/2007: Using the default page size (4K), we're
        // unable to munmap any pages once we realize we've run out.  Seems
        // like an OS bug -- larger pages works fine.  I attempted to use as
        // few 4K pages as possible (determined empirically -- the address
        // space includes libraries that may vary across machines or builds)
        // and it was still unable to munmap any pages once it ran out.
        params.cbPage = 32 * 1024;

        params.nMemPagesMax = 
            computeMaxPagesUpperBound(addrSpaceSize, params.cbPage);
        params.nMemPagesInit = params.nMemPagesMax;

        BOOST_CHECK_NO_THROW(pCache = Cache::newCache(params));
        BOOST_CHECK_EQUAL(
            CacheParams::defaultMemPagesMax,
            pCache->getMaxAllocatedPageCount());

        // See note above; same logic.
        BOOST_CHECK_EQUAL(
            CacheParams::defaultMemPagesMax,
            pCache->getAllocatedPageCount());

        pCache.reset();

        restoreAddressSpaceLimit(savedLimits);
    }

    void testLargeCacheRequest()
    {
        struct rlimit savedLimits;

        rlim_t addrSpaceSize = 
            setAddressSpaceLimit(SMALL_ADDR_SPACE, savedLimits);

        CacheParams params;

        // REVIEW: SWZ: 10/11/2007: Same page size weirdness as in
        // testLargeCacheInit.
        params.cbPage = 32 * 1024;

        params.nMemPagesMax = 
            computeMaxPagesUpperBound(addrSpaceSize, params.cbPage);
        params.nMemPagesInit = 
            (params.nMemPagesMax / 2) < 1000
            ? (params.nMemPagesMax / 2) 
            : 1000;

        BOOST_CHECK_NO_THROW(pCache = Cache::newCache(params));
        BOOST_CHECK_EQUAL(
            params.nMemPagesMax,
            pCache->getMaxAllocatedPageCount());
        BOOST_CHECK_EQUAL(
            params.nMemPagesInit,
            pCache->getAllocatedPageCount());

        BOOST_CHECK_THROW(
            pCache->setAllocatedPageCount(params.nMemPagesMax),
            std::exception);
        BOOST_CHECK_EQUAL(
            params.nMemPagesMax,
            pCache->getMaxAllocatedPageCount());
        BOOST_CHECK_EQUAL(
            params.nMemPagesInit,
            pCache->getAllocatedPageCount());

        pCache.reset();

        restoreAddressSpaceLimit(savedLimits);
    }
#endif // RLIMIT_AS
};

FENNEL_UNIT_TEST_SUITE(CacheTest);

// End CacheTest.cpp
