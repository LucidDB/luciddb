/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
