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
