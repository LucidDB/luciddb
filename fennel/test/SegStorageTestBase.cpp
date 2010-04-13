/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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
#include "fennel/test/SegStorageTestBase.h"
#include "fennel/device/RandomAccessDevice.h"
#include "fennel/segment/LinearDeviceSegment.h"

using namespace fennel;

void SegStorageTestBase::openStorage(DeviceMode openMode)
{
    CacheTestBase::openStorage(openMode);
    openSegmentStorage(openMode);
}

void SegStorageTestBase::openSegmentStorage(DeviceMode openMode)
{
    pLinearSegment = createLinearDeviceSegment(
        dataDeviceId,
        openMode.create ? 0
        : pRandomAccessDevice->getSizeInBytes() / cbPageFull);
}

void SegStorageTestBase::openRandomSegment()
{
    // reopen will interpret pages as already allocated, which
    // is what RandomAllocationSegment expects
    closeStorage();
    openStorage(DeviceMode::load);
    pRandomSegment = pSegmentFactory->newRandomAllocationSegment(
        pLinearSegment, true);
    pLinearSegment.reset();
}

SharedSegment SegStorageTestBase::createLinearDeviceSegment(
    DeviceId deviceId, uint nPages)
{
    BlockId blockId(0);
    CompoundId::setDeviceId(blockId, deviceId);
    CompoundId::setBlockNum(blockId, 0);
    LinearDeviceSegmentParams deviceParams;
    deviceParams.firstBlockId = blockId;
    deviceParams.nPagesMin = nPages;
    deviceParams.nPagesAllocated = nPages;
    return pSegmentFactory->newLinearDeviceSegment(
        pCache, deviceParams);
}

void SegStorageTestBase::closeLinearSegment()
{
    if (pLinearSegment) {
        assert(pLinearSegment.unique());
        pLinearSegment.reset();
    }
}

void SegStorageTestBase::closeRandomSegment()
{
    if (pRandomSegment) {
        assert(pRandomSegment.unique());
        pRandomSegment.reset();
    }
}

void SegStorageTestBase::closeVersionedRandomSegment()
{
    if (pVersionedRandomSegment) {
        assert(pVersionedRandomSegment.unique());
        pVersionedRandomSegment.reset();
    }
}

void SegStorageTestBase::closeSnapshotRandomSegment()
{
    if (pSnapshotRandomSegment) {
        assert(pSnapshotRandomSegment.unique());
        pSnapshotRandomSegment.reset();
    }
}

void SegStorageTestBase::closeStorage()
{
    closeLinearSegment();
    closeRandomSegment();
    closeVersionedRandomSegment();
    // TODO:  assert pSegmentFactory.unique(), but not here
    CacheTestBase::closeStorage();
}

SegStorageTestBase::SegStorageTestBase()
{
    pSegmentFactory =
        SegmentFactory::newSegmentFactory(configMap, shared_from_this());
}

// End SegStorageTestBase.cpp
