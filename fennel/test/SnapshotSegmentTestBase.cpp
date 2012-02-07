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
#include "fennel/test/SnapshotSegmentTestBase.h"
#include "fennel/segment/VersionedRandomAllocationSegment.h"
#include "fennel/segment/SnapshotRandomAllocationSegment.h"
#include "fennel/segment/LinearViewSegment.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

SnapshotSegmentTestBase::SnapshotSegmentTestBase()
{
    nDiskPagesTotal = nDiskPages;
    tempDeviceId = DeviceId(42);
}

void SnapshotSegmentTestBase::testCaseSetUp()
{
    currCsn = TxnId(0);
    commit = true;
    updatedCsns.clear();
}

void SnapshotSegmentTestBase::openSegmentStorage(DeviceMode openMode)
{
    nDiskPages = nDiskPagesTotal;
    if (openMode.create) {
        firstPageId = NULL_PAGE_ID;
    }

    pTempDevice =
        openDevice("temp.dat", openMode, nDiskPages / 50, tempDeviceId);
    SharedSegment pTempDeviceSegment =
        createLinearDeviceSegment(tempDeviceId, nDiskPages / 50);
    pTempSegment =
        pSegmentFactory->newRandomAllocationSegment(
            pTempDeviceSegment,
            openMode.create);

    SharedSegment pDeviceSegment =
        createLinearDeviceSegment(dataDeviceId, nDiskPages);
    pVersionedRandomSegment =
        pSegmentFactory->newVersionedRandomAllocationSegment(
            pDeviceSegment,
            pTempSegment,
            openMode.create);
    pSnapshotRandomSegment =
        pSegmentFactory->newSnapshotRandomAllocationSegment(
            pVersionedRandomSegment,
            pVersionedRandomSegment,
            currCsn);
    setForceCacheUnmap(pSnapshotRandomSegment);

    pRandomSegment = pSnapshotRandomSegment;

    nDiskPages /= 2;
    SharedSegment pLinearViewSegment =
        pSegmentFactory->newLinearViewSegment(
            pSnapshotRandomSegment,
            firstPageId);
    pLinearSegment = pLinearViewSegment;
}

void SnapshotSegmentTestBase::setForceCacheUnmap(SharedSegment pSegment)
{
    // Force the snapshot segment to always execute its checkpoints during
    // a cache flush and unmap, in order to unmap these page from the cache
    if (pSegment) {
        SnapshotRandomAllocationSegment *pSnapshotSegment =
            SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
                pSegment);
        pSnapshotSegment->setForceCacheUnmap();
    }
}

void SnapshotSegmentTestBase::closeStorage()
{
    commitChanges(currCsn);
    closeLinearSegment();
    pRandomSegment.reset();
    closeSnapshotRandomSegment();
    if (pSnapshotRandomSegment2) {
        assert(pSnapshotRandomSegment2.unique());
        pSnapshotRandomSegment2.reset();
    }
    // Free leftover temp pages used during page versioning
    if (pVersionedRandomSegment) {
        VersionedRandomAllocationSegment *pVRSegment =
            SegmentFactory::dynamicCast<VersionedRandomAllocationSegment *>(
                pVersionedRandomSegment);
        pVRSegment->freeTempPages();
    }
    closeVersionedRandomSegment();
    if (pTempSegment) {
        // Confirm that all temp pages have been freed.
        BOOST_REQUIRE(pTempSegment->getAllocatedSizeInPages() == 0);
        assert(pTempSegment.unique());
        pTempSegment.reset();
    }
    if (pTempDevice) {
        closeDevice(tempDeviceId, pTempDevice);
    }
    SegmentTestBase::closeStorage();
}

void SnapshotSegmentTestBase::testAllocateAll()
{
    SegmentTestBase::testAllocateAll();
    assert(firstPageId == NULL_PAGE_ID);
    LinearViewSegment *pLinearViewSegment =
        SegmentFactory::dynamicCast<LinearViewSegment *>(pLinearSegment);
    assert(pLinearViewSegment);
    firstPageId = pLinearViewSegment->getFirstPageId();
}

void SnapshotSegmentTestBase::verifyPage(CachePage &page, uint x)
{
    // If the pageId is a multiple of one of the csn's smaller than the
    // current csn, then the page should reflect the update made by
    // that smaller csn
    uint update = 0;
    for (int i = updatedCsns.size() - 1; i >= 0; i--) {
        if (updatedCsns[i] <= currCsn
            && x % opaqueToInt(updatedCsns[i]) == 0)
        {
            update = opaqueToInt(updatedCsns[i]);
            break;
        }
    }
    SegmentTestBase::verifyPage(page, x + update);
}

void SnapshotSegmentTestBase::fillPage(CachePage &page, uint x)
{
    SegmentTestBase::fillPage(page, x + opaqueToInt(currCsn));
}

void SnapshotSegmentTestBase::commitChanges(TxnId commitCsn)
{
    if (pSnapshotRandomSegment) {
        SnapshotRandomAllocationSegment *pSnapshotSegment =
            SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
                pSnapshotRandomSegment);
        if (commit) {
            pSnapshotSegment->commitChanges(commitCsn);
        } else {
            pSnapshotSegment->rollbackChanges();
        }
    }
}

// End SnapshotSegmentTestBase.cpp
