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
        FENNEL_UNIT_TEST_CASE(SegPageIterTest,testUnboundedIter);
        FENNEL_UNIT_TEST_CASE(SegPageIterTest,testBoundedIter);
        FENNEL_UNIT_TEST_CASE(SegPageIterTest,testWithLock);
        FENNEL_UNIT_TEST_CASE(SegPageIterTest,testHighPrefetchRejects);
        FENNEL_UNIT_TEST_CASE(SegPageIterTest,testLowPrefetchRejects);
    }

    void testUnboundedIter()
    {
        testIter(FIRST_LINEAR_PAGE_ID,NULL_PAGE_ID,false,0);
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
        testIter(FIRST_LINEAR_PAGE_ID,NULL_PAGE_ID,true,0);
    }

    void testHighPrefetchRejects()
    {
        // High pre-fetch reject rate.  This will force frequent downward
        // throttles.
        testIter(FIRST_LINEAR_PAGE_ID,NULL_PAGE_ID,false,3);
    }

    void testLowPrefetchRejects()
    {
        // Low pre-fetch reject rate.  This will allow the rate to throttle
        // back up once it's throttled down.
        testIter(FIRST_LINEAR_PAGE_ID,NULL_PAGE_ID,false,123);
    }

    void testIter(PageId beginPageId,PageId endPageId,bool bLock,int rejectRate)
    {
        openStorage(DeviceMode::createNew);

        // reopen will interpret pages as already allocated
        closeStorage();
        openStorage(DeviceMode::load);

        SegmentAccessor segmentAccessor(pLinearSegment,pCache);
        SegPageLock pageLock(segmentAccessor);
        SegPageIter iter;
        iter.mapRange(segmentAccessor,beginPageId,endPageId);
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
