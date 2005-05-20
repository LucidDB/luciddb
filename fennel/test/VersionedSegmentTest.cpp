/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
            "shadow.dat",openMode,
            2*nDiskPages,logDeviceId);
        SharedSegment pLogSegment = createLinearDeviceSegment(
            logDeviceId,2*nDiskPages);
        SharedSegment pCircularSegment = pSegmentFactory->newCircularSegment(
            pLogSegment,SharedCheckpointProvider(),firstLogPageId);
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
            closeDevice(logDeviceId,pLogDevice);
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
        FENNEL_UNIT_TEST_CASE(SegmentTestBase,testSingleThread);
        FENNEL_UNIT_TEST_CASE(VersionedSegmentTest,testRecovery);
        FENNEL_UNIT_TEST_CASE(PagingTestBase,testMultipleThreads);
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
        pVersionedSegment->recover(firstLogPageId);
        testSequentialRead();
        closeStorage();
        firstLogPageId = NULL_PAGE_ID;
    }
    
    virtual void fillPage(CachePage &page,uint x)
    {
        SegmentTestBase::fillPage(page,x+versionNumber);
    }

    virtual void testCheckpoint()
    {
        pLinearSegment->checkpoint(CHECKPOINT_FLUSH_FUZZY);
        ++versionNumber;
        VersionedSegment *pVersionedSegment =
            SegmentFactory::dynamicCast<VersionedSegment *>(pLinearSegment);
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
        SegmentTestBase::verifyPage(page,x+pageVersion);
    }
};

FENNEL_UNIT_TEST_SUITE(VersionedSegmentTest);

// End VersionedSegmentTest.cpp
