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
#include "fennel/segment/SegPageIter.h"
#include "fennel/test/SegStorageTestBase.h"
#include "fennel/segment/Segment.h"
#include "fennel/segment/SegPageLock.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class SegPageIterTest : virtual public SegStorageTestBase
{
public:
    explicit SegPageIterTest()
    {
        FENNEL_UNIT_TEST_CASE(SegPageIterTest, testUnboundedIter);
        FENNEL_UNIT_TEST_CASE(SegPageIterTest, testBoundedIter);
        FENNEL_UNIT_TEST_CASE(SegPageIterTest, testWithLock);
        FENNEL_UNIT_TEST_CASE(SegPageIterTest, testHighPrefetchRejects);
        FENNEL_UNIT_TEST_CASE(SegPageIterTest, testLowPrefetchRejects);
    }

    void testUnboundedIter()
    {
        testIter(FIRST_LINEAR_PAGE_ID, NULL_PAGE_ID, false, 0);
    }

    void testBoundedIter()
    {
        testIter(
            Segment::getLinearPageId(3),
            Segment::getLinearPageId(51),
            false,
            0);
    }

    void testWithLock()
    {
        testIter(FIRST_LINEAR_PAGE_ID, NULL_PAGE_ID, true, 0);
    }

    void testHighPrefetchRejects()
    {
        // High pre-fetch reject rate.  This will force frequent downward
        // throttles.
        testIter(FIRST_LINEAR_PAGE_ID, NULL_PAGE_ID, false, 3);
    }

    void testLowPrefetchRejects()
    {
        // Low pre-fetch reject rate.  This will allow the rate to throttle
        // back up once it's throttled down.
        testIter(FIRST_LINEAR_PAGE_ID, NULL_PAGE_ID, false, 123);
    }

    void testIter(
        PageId beginPageId, PageId endPageId, bool bLock, int rejectRate)
    {
        openStorage(DeviceMode::createNew);

        // reopen will interpret pages as already allocated
        closeStorage();
        openStorage(DeviceMode::load);

        SegmentAccessor segmentAccessor(pLinearSegment, pCache);
        SegPageLock pageLock(segmentAccessor);
        SegPageIter iter;
        iter.mapRange(segmentAccessor, beginPageId, endPageId);
        PageId pageId = beginPageId;
        for (uint i = 0; ; i++) {
            BOOST_CHECK_EQUAL(pageId,*iter);
            if (pageId == endPageId) {
                break;
            }
            if (rejectRate > 0 && !(i % rejectRate)) {
                iter.forcePrefetchReject();
            }
            if (bLock) {
                pageLock.lockShared(pageId);
            }
            BOOST_CHECK(pageId != NULL_PAGE_ID);
            ++iter;
            pageId = pLinearSegment->getPageSuccessor(pageId);
        }
        iter.makeSingular();
        pageLock.unlock();
    }
};

FENNEL_UNIT_TEST_SUITE(SegPageIterTest);

// End SegPageIterTest.cpp
