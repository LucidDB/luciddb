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
#include "fennel/segment/LinearDeviceSegment.h"
#include "fennel/device/RandomAccessDevice.h"
#include "fennel/cache/Cache.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LinearDeviceSegmentParams::LinearDeviceSegmentParams()
{
    firstBlockId = NULL_BLOCK_ID;
    nPagesMin = 0;
    nPagesAllocated = 0;
    nPagesIncrement = 1;
    nPagesMax = MAXU;
}

BlockNum LinearDeviceSegment::getAvailableDevicePages() const
{
    return pDevice->getSizeInBytes() / getFullPageSize()
        - CompoundId::getBlockNum(firstBlockId);
}

LinearDeviceSegment::LinearDeviceSegment(
    SharedCache cache,
    LinearDeviceSegmentParams const &params)
    : Segment(cache),
      pDevice(
          cache->getDevice(CompoundId::getDeviceId(params.firstBlockId)))
{
    firstBlockId = params.firstBlockId;
    nPagesMax = params.nPagesMax;
    nPagesAllocated = params.nPagesAllocated;
    nPagesIncrement = params.nPagesIncrement;
    nPagesExtended = 0;
    BlockNum nPagesActual = getAvailableDevicePages();
    if (nPagesActual < params.nPagesMin) {
        pDevice->setSizeInBytes(
            pDevice->getSizeInBytes()
            + (params.nPagesMin - nPagesActual) * getFullPageSize());
        nPagesActual = params.nPagesMin;
    }
    if (isMAXU(nPagesAllocated)) {
        nPagesAllocated = nPagesActual;
    } else {
        assert(nPagesAllocated <= nPagesActual);
    }
}

LinearDeviceSegment::~LinearDeviceSegment()
{
}

BlockId LinearDeviceSegment::translatePageId(PageId pageId)
{
    assert(isPageIdAllocated(pageId));
    BlockId blockId = firstBlockId;
    CompoundId::setBlockNum(
        blockId,
        CompoundId::getBlockNum(firstBlockId)
        + getLinearBlockNum(pageId));
    return blockId;
}

PageId LinearDeviceSegment::translateBlockId(BlockId blockId)
{
    return getLinearPageId(
        CompoundId::getBlockNum(blockId)
        - CompoundId::getBlockNum(firstBlockId));
}

BlockNum LinearDeviceSegment::getAllocatedSizeInPages()
{
    return nPagesAllocated;
}

BlockNum LinearDeviceSegment::getNumPagesOccupiedHighWater()
{
    return getAllocatedSizeInPages();
}

BlockNum LinearDeviceSegment::getNumPagesExtended()
{
    return nPagesExtended;
}

PageId LinearDeviceSegment::allocatePageId(PageOwnerId)
{
    // nothing to do with PageOwnerId

    BlockNum newBlockNum = nPagesAllocated;

    if (!ensureAllocatedSize(nPagesAllocated + 1)) {
        return NULL_PAGE_ID;
    }

    return getLinearPageId(newBlockNum);
}

void LinearDeviceSegment::deallocatePageRange(
    PageId startPageId, PageId endPageId)
{
    if (endPageId != NULL_PAGE_ID) {
        // REVIEW:  Technically, this should assert; instead, we let it slip so
        // that LinearDeviceSegments can be used as really stupid logs in
        // tests.  Should probably fix the tests and tighten this up.
        return;
    }
    if (startPageId == NULL_PAGE_ID) {
        nPagesAllocated = 0;
    } else {
        nPagesAllocated = getLinearBlockNum(startPageId);
    }
}

bool LinearDeviceSegment::isPageIdAllocated(PageId pageId)
{
    return isLinearPageIdAllocated(pageId);
}

PageId LinearDeviceSegment::getPageSuccessor(PageId pageId)
{
    return getLinearPageSuccessor(pageId);
}

void LinearDeviceSegment::setPageSuccessor(PageId pageId, PageId successorId)
{
    setLinearPageSuccessor(pageId, successorId);
}

Segment::AllocationOrder LinearDeviceSegment::getAllocationOrder() const
{
    return LINEAR_ALLOCATION;
}

DeviceId LinearDeviceSegment::getDeviceId() const
{
    return CompoundId::getDeviceId(firstBlockId);
}

bool LinearDeviceSegment::ensureAllocatedSize(BlockNum nPages)
{
    if (nPages > nPagesMax) {
        return false;
    }
    if (nPages <= nPagesAllocated) {
        return true;
    }
    BlockNum nPagesAvailable = getAvailableDevicePages();
    assert(nPagesAllocated <= nPagesAvailable);
    if (nPages > nPagesAvailable) {
        if (!nPagesIncrement) {
            return false;
        }
        BlockNum nNewPages =
            std::max(nPagesIncrement, nPages - nPagesAvailable);
        if (!isMAXU(nPagesMax) && (nPagesAvailable + nNewPages > nPagesMax)) {
            nNewPages = nPagesMax - nPagesAvailable;
        }
        assert(nNewPages);
        nPagesExtended += nNewPages;
        pDevice->setSizeInBytes(
            pDevice->getSizeInBytes() + nNewPages*getFullPageSize());
    }
    nPagesAllocated = nPages;
    return true;
}

void LinearDeviceSegment::delegatedCheckpoint(
    Segment &delegatingSegment,
    CheckpointType checkpointType)
{
    Segment::delegatedCheckpoint(delegatingSegment, checkpointType);
    if (checkpointType != CHECKPOINT_DISCARD) {
        pDevice->flush();
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LinearDeviceSegment.cpp
