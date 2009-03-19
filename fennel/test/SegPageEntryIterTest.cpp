/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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
#include "fennel/segment/SegPageEntryIterImpl.h"
#include "fennel/segment/MockSegPageEntryIterSource.h"
#include "fennel/test/SegStorageTestBase.h"
#include "fennel/segment/Segment.h"
#include "fennel/segment/SegPageLock.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class SegPageEntryIterTest : virtual public SegStorageTestBase
{
public:
    explicit SegPageEntryIterTest()
    {
        FENNEL_UNIT_TEST_CASE(SegPageEntryIterTest, testUnboundedIter);
        FENNEL_UNIT_TEST_CASE(SegPageEntryIterTest, testBoundedIter);
        FENNEL_UNIT_TEST_CASE(SegPageEntryIterTest, testWithLock);
        FENNEL_UNIT_TEST_CASE(SegPageEntryIterTest, testNoPrefetch);
        FENNEL_UNIT_TEST_CASE(SegPageEntryIterTest, testReject);
        FENNEL_UNIT_TEST_CASE(SegPageEntryIterTest, testRejectNoPrefetch);
        FENNEL_UNIT_TEST_CASE(SegPageEntryIterTest, testQueueSize1);
        FENNEL_UNIT_TEST_CASE(SegPageEntryIterTest, testRejectQueueSize1);
    }

    void testUnboundedIter()
    {
        testIter(FIRST_LINEAR_PAGE_ID, NULL_PAGE_ID, false, -1, 0);
    }

    void testBoundedIter()
    {
        testIter(
            Segment::getLinearPageId(3),
            Segment::getLinearPageId(51),
            false,
            -1,
            0);
    }

    void testWithLock()
    {
        testIter(FIRST_LINEAR_PAGE_ID, NULL_PAGE_ID, true, -1, 0);
    }

    void testNoPrefetch()
    {
        testIter(FIRST_LINEAR_PAGE_ID, NULL_PAGE_ID, false, 0, 0);
    }

    void testReject()
    {
        testIter(FIRST_LINEAR_PAGE_ID, NULL_PAGE_ID, false, -1, 251);
    }

    void testRejectNoPrefetch()
    {
        testIter(FIRST_LINEAR_PAGE_ID, NULL_PAGE_ID, false, 0, 19);
    }

    void testQueueSize1()
    {
        testIter(FIRST_LINEAR_PAGE_ID, NULL_PAGE_ID, false, 1, 0);
    }

    void testRejectQueueSize1()
    {
        testIter(FIRST_LINEAR_PAGE_ID, NULL_PAGE_ID, false, 1, 7);
    }

    void testIter(
        PageId beginPageId, PageId endPageId, bool bLock, int queueSize,
        uint rejectRate)
    {
        char buf[10];
        if (queueSize >= 0) {
            sprintf(buf, "%d", queueSize);
            configMap.setStringParam(CacheParams::paramPrefetchPagesMax, buf);
            cacheParams.readConfig(configMap);
        }
        openStorage(DeviceMode::createNew);

        // reopen will interpret pages as already allocated
        closeStorage();
        openStorage(DeviceMode::load);

        SegmentAccessor segmentAccessor(pLinearSegment, pCache);
        SegPageLock pageLock(segmentAccessor);
        MockSegPageEntryIterSource prefetchSource(
            segmentAccessor, beginPageId);
        SegPageEntryIter<int> iter(20);
        iter.setPrefetchSource(prefetchSource);
        iter.mapRange(segmentAccessor, NULL_PAGE_ID, endPageId);
        PageId pageId = beginPageId;
        for (int i = 0; ; i++) {
            std::pair<PageId, int> &entry = *iter;
            BOOST_CHECK_EQUAL(pageId, entry.first);
            BOOST_CHECK_EQUAL(i, entry.second);
            if (pageId == endPageId) {
                break;
            }
            if (bLock) {
                pageLock.lockShared(pageId);
            }
            BOOST_CHECK(pageId != NULL_PAGE_ID);
            if (rejectRate > 0 && !(i % rejectRate)) {
                iter.forcePrefetchReject();
            }
            ++iter;
            // MockSegPageEntryIterSource returns every other page twice
            if (i % 2) {
                pageId = pLinearSegment->getPageSuccessor(pageId);
                if (pageId != endPageId) {
                    pageId = pLinearSegment->getPageSuccessor(pageId);
                }
            }
        }
        iter.makeSingular();
        pageLock.unlock();

        // reset back to default value
        if (queueSize >= 0) {
            sprintf(buf, "%d", CacheParams::defaultPrefetchPagesMax);
            configMap.setStringParam(CacheParams::paramPrefetchPagesMax, buf);
            cacheParams.readConfig(configMap);
        }
    }
};

FENNEL_UNIT_TEST_SUITE(SegPageEntryIterTest);

// End SegPageEntryIterTest.cpp
