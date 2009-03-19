/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2008-2008 The Eigenbase Project
// Copyright (C) 2008-2008 SQLstream, Inc.
// Copyright (C) 2008-2008 LucidEra, Inc.
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
#include "fennel/test/SegmentTestBase.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/cache/Cache.h"
#include "fennel/cache/CacheStats.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class RandomAllocationSegmentTest : virtual public SegmentTestBase
{
    // TODO jvs 13-Jan-2008:  Need many more tests here...

    void testAllocateAndDeallocate();

public:
    explicit RandomAllocationSegmentTest()
    {
        FENNEL_UNIT_TEST_CASE(
            RandomAllocationSegmentTest, testAllocateAndDeallocate);
    }
};

void RandomAllocationSegmentTest::testAllocateAndDeallocate()
{
    openStorage(DeviceMode::createNew);
    openRandomSegment();

    // Allocate 100 pages
    std::vector<PageId> pageList;
    uint n = 100;
    SegmentAccessor segAccessor(pRandomSegment, pCache);
    SegPageLock segPageLock(segAccessor);
    for (uint i = 0; i < n; ++i) {
        PageId pageId = segPageLock.allocatePage();
        BOOST_CHECK(pRandomSegment->isPageIdAllocated(pageId));
        pageList.push_back(pageId);

        // Perform a dummy write
        segPageLock.getPage().getWritableData();
        segPageLock.unlock();
    }

    // Verify that all are still allocated
    for (uint i = 0; i < n; ++i) {
        BOOST_CHECK(pRandomSegment->isPageIdAllocated(pageList[i]));
    }

    // Verify that the page count matches what's been allocated
    uint nAllocated = pRandomSegment->getAllocatedSizeInPages();
    BOOST_REQUIRE(nAllocated == n);

    // Verify that the high water page occupied count exceeds the allocated
    // count
    uint highWaterMarkBefore = pRandomSegment->getNumPagesOccupiedHighWater();
    BOOST_REQUIRE(highWaterMarkBefore > n);

    // Save cache stats before deallocation
    CacheStats statsBefore;
    pCache->collectStats(statsBefore);

    // Deallocate all
    for (uint i = 0; i < n; ++i) {
        PageId pageId = pageList[i];
        pRandomSegment->deallocatePageRange(pageId, pageId);
        BOOST_CHECK(!pRandomSegment->isPageIdAllocated(pageId));
    }

    // Make sure the high water mark stays the same even after deallocation
    uint highWaterMarkAfter = pRandomSegment->getNumPagesOccupiedHighWater();
    BOOST_REQUIRE(highWaterMarkAfter == highWaterMarkBefore);

    // Get cache stats after deallocation and compare
    CacheStats statsAfter;
    pCache->collectStats(statsAfter);

    // Count of unused pages should go up since deallocation results
    // in discard.
    BOOST_CHECK(statsAfter.nMemPagesUnused > statsBefore.nMemPagesUnused);

    // Verify that all are still deallocated
    for (uint i = 0; i < n; ++i) {
        BOOST_CHECK(!pRandomSegment->isPageIdAllocated(pageList[i]));
    }

    // Allocate the pages again, and recheck the highwater mark to make
    // sure it's still the same
    for (uint i = 0; i < n; ++i) {
        PageId pageId = segPageLock.allocatePage();
    }
    highWaterMarkAfter = pRandomSegment->getNumPagesOccupiedHighWater();
    BOOST_REQUIRE(highWaterMarkAfter == highWaterMarkBefore);
}

FENNEL_UNIT_TEST_SUITE(RandomAllocationSegmentTest);

// End RandomAllocationSegmentTest.cpp
