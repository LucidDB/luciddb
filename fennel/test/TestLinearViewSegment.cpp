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
#include "fennel/test/SegmentTestBase.h"
#include "fennel/segment/RandomAllocationSegment.h"
#include "fennel/segment/LinearViewSegment.h"

using namespace fennel;

class TestLinearViewSegment : virtual public SegmentTestBase
{
    uint nDiskPagesTotal;
    PageId firstPageId;
    
public:
    explicit TestLinearViewSegment()
    {
        nDiskPagesTotal = nDiskPages;
        FENNEL_UNIT_TEST_CASE(SegmentTestBase,testSingleThread);
        FENNEL_UNIT_TEST_CASE(PagingTestBase,testMultipleThreads);
    }
  
    virtual void openSegmentStorage(DeviceMode openMode)
    {
        nDiskPages = nDiskPagesTotal;
        if (openMode.create) {
            firstPageId = NULL_PAGE_ID;
        }
        SharedSegment pDeviceSegment = createLinearDeviceSegment(
            dataDeviceId,nDiskPages);
        pRandomSegment = pSegmentFactory->newRandomAllocationSegment(
            pDeviceSegment,openMode.create);
        nDiskPages /= 2;
        SharedSegment pLinearViewSegment =
            pSegmentFactory->newLinearViewSegment(pRandomSegment,firstPageId);
        pLinearSegment = pLinearViewSegment;
    }
    
    virtual void testAllocateAll()
    {
        SegmentTestBase::testAllocateAll();
        assert(firstPageId == NULL_PAGE_ID);
        LinearViewSegment *pLinearViewSegment =
            SegmentFactory::dynamicCast<LinearViewSegment *>(pLinearSegment);
        assert(pLinearViewSegment);
        firstPageId = pLinearViewSegment->getFirstPageId();
    }
};

FENNEL_UNIT_TEST_SUITE(TestLinearViewSegment);

// End TestLinearViewSegment.cpp
