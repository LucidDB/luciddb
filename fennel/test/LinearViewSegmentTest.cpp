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
#include "fennel/segment/RandomAllocationSegment.h"
#include "fennel/segment/LinearViewSegment.h"

using namespace fennel;

class LinearViewSegmentTest : virtual public SegmentTestBase
{
    uint nDiskPagesTotal;
    PageId firstPageId;

public:
    explicit LinearViewSegmentTest()
    {
        nDiskPagesTotal = nDiskPages;
        FENNEL_UNIT_TEST_CASE(SegmentTestBase, testSingleThread);
        FENNEL_UNIT_TEST_CASE(PagingTestBase, testMultipleThreads);
    }

    virtual void openSegmentStorage(DeviceMode openMode)
    {
        nDiskPages = nDiskPagesTotal;
        if (openMode.create) {
            firstPageId = NULL_PAGE_ID;
        }
        SharedSegment pDeviceSegment = createLinearDeviceSegment(
            dataDeviceId, nDiskPages);
        pRandomSegment = pSegmentFactory->newRandomAllocationSegment(
            pDeviceSegment, openMode.create);
        nDiskPages /= 2;
        SharedSegment pLinearViewSegment =
            pSegmentFactory->newLinearViewSegment(pRandomSegment, firstPageId);
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

FENNEL_UNIT_TEST_SUITE(LinearViewSegmentTest);

// End LinearViewSegmentTest.cpp
