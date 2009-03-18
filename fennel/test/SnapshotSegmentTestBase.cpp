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
        if (updatedCsns[i] <= currCsn &&
            x % opaqueToInt(updatedCsns[i]) == 0)
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
