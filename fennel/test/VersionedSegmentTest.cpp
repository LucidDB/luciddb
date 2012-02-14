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
#include "fennel/segment/VersionedSegment.h"
#include "fennel/segment/WALSegment.h"

using namespace fennel;

class VersionedSegmentTest : virtual public SegmentTestBase
{
    SegVersionNum versionNumber;
    DeviceId logDeviceId;
    SharedRandomAccessDevice pLogDevice;
    PageId firstLogPageId;
    PseudoUuid onlineUuid;

public:
    virtual void openSegmentStorage(DeviceMode openMode)
    {
        SegmentTestBase::openSegmentStorage(openMode);

        // NOTE:  2*nDiskPages for fuzzy checkpointing
        pLogDevice = openDevice(
            "shadow.dat", openMode,
            2*nDiskPages,logDeviceId);
        SharedSegment pLogSegment = createLinearDeviceSegment(
            logDeviceId, 2 * nDiskPages);
        SharedSegment pCircularSegment = pSegmentFactory->newCircularSegment(
            pLogSegment, SharedCheckpointProvider(), firstLogPageId);
        SharedSegment pWALSegment = pSegmentFactory->newWALSegment(
            pCircularSegment);
        SharedSegment pVersionedSegment = pSegmentFactory->newVersionedSegment(
            pLinearSegment,
            pWALSegment,
            onlineUuid,
            versionNumber);
        pLinearSegment = pVersionedSegment;
    }

    virtual void closeStorage()
    {
        closeLinearSegment();
        closeRandomSegment();
        if (pLogDevice) {
            closeDevice(logDeviceId, pLogDevice);
        }
        SegmentTestBase::closeStorage();
        ++versionNumber;
    }

    explicit VersionedSegmentTest()
    {
        logDeviceId = DeviceId(42);
        versionNumber = 0;
        firstLogPageId = NULL_PAGE_ID;
        onlineUuid.generateInvalid();
        FENNEL_UNIT_TEST_CASE(SegmentTestBase, testSingleThread);
        FENNEL_UNIT_TEST_CASE(VersionedSegmentTest, testRecovery);
        FENNEL_UNIT_TEST_CASE(PagingTestBase, testMultipleThreads);
    }

    void testRecovery()
    {
        // revert to previous version
        versionNumber -= 2;
        firstLogPageId = FIRST_LINEAR_PAGE_ID;
        openStorage(DeviceMode::load);
        VersionedSegment *pVersionedSegment =
            SegmentFactory::dynamicCast<VersionedSegment *>(pLinearSegment);
        assert(pVersionedSegment);
        pVersionedSegment->recover(pLinearSegment, firstLogPageId);
        testSequentialRead();
        closeStorage();
        firstLogPageId = NULL_PAGE_ID;
    }

    virtual void fillPage(CachePage &page,uint x)
    {
        SegmentTestBase::fillPage(page, x + versionNumber);
    }

    virtual void testCheckpoint()
    {
        pLinearSegment->checkpoint(CHECKPOINT_FLUSH_FUZZY);
        ++versionNumber;
        VersionedSegment *pVersionedSegment =
            SegmentFactory::dynamicCast<VersionedSegment *>(pLinearSegment);
        assert(pVersionedSegment);
        assert(versionNumber == pVersionedSegment->getVersionNumber());
        pVersionedSegment->deallocateCheckpointedLog(CHECKPOINT_FLUSH_FUZZY);
    }

    virtual void verifyPage(CachePage &page,uint x)
    {
        VersionedSegment *pVersionedSegment =
            SegmentFactory::dynamicCast<VersionedSegment *>(pLinearSegment);
        assert(pVersionedSegment);
        SegVersionNum pageVersion = pVersionedSegment->getPageVersion(page);
        assert(pageVersion <= versionNumber);
        SegmentTestBase::verifyPage(page, x + pageVersion);
    }
};

FENNEL_UNIT_TEST_SUITE(VersionedSegmentTest);

// End VersionedSegmentTest.cpp
