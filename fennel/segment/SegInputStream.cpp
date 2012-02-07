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
#include "fennel/segment/SegInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedSegInputStream SegInputStream::newSegInputStream(
    SegmentAccessor const &segmentAccessor,
    PageId beginPageId)
{
    return SharedSegInputStream(
        new SegInputStream(segmentAccessor, beginPageId),
        ClosableObjectDestructor());
}

SegInputStream::SegInputStream(
    SegmentAccessor const &segmentAccessor,
    PageId beginPageId,
    uint cbExtraHeader)
    : SegStream(segmentAccessor, cbExtraHeader)
{
    if (beginPageId == FIRST_LINEAR_PAGE_ID) {
        assert(
            getSegment()->getAllocationOrder() == Segment::LINEAR_ALLOCATION);
    }
    currPageId = beginPageId;
    shouldDeallocate = false;
}

void SegInputStream::startPrefetch()
{
    pageIter.mapRange(segmentAccessor, currPageId);
}

void SegInputStream::endPrefetch()
{
    pageIter.makeSingular();
}

void SegInputStream::lockBuffer()
{
    pageLock.lockShared(currPageId);
    SegStreamNode const &node = pageLock.getNodeForRead();
    PConstBuffer pFirstByte =
        reinterpret_cast<PConstBuffer>(&node) + cbPageHeader;
    setBuffer(pFirstByte, node.cbData);
}

void SegInputStream::readNextBuffer()
{
    nullifyBuffer();
    if (pageLock.isLocked()) {
        if (currPageId != NULL_PAGE_ID) {
            if (pageIter.isSingular()) {
                currPageId = getSegment()->getPageSuccessor(currPageId);
            } else {
                ++pageIter;
                assert(*pageIter == getSegment()->getPageSuccessor(currPageId));
                currPageId = *pageIter;
            }
        }
        if (shouldDeallocate) {
            pageLock.deallocateLockedPage();
        } else {
            pageLock.unlock();
        }
    }
    if (currPageId == NULL_PAGE_ID) {
        return;
    }
    lockBuffer();
}

void SegInputStream::readPrevBuffer()
{
    assert(pageIter.isSingular());
    assert(
        getSegment()->getAllocationOrder() >= Segment::CONSECUTIVE_ALLOCATION);
    nullifyBuffer();
    if (Segment::getLinearBlockNum(currPageId) == 0) {
        return;
    }
    --currPageId;
    lockBuffer();
}

void SegInputStream::closeImpl()
{
    pageIter.makeSingular();
    if (shouldDeallocate) {
        pageLock.unlock();
        while (currPageId != NULL_PAGE_ID) {
            PageId nextPageId = getSegment()->getPageSuccessor(currPageId);
            pageLock.deallocateUnlockedPage(currPageId);
            currPageId = nextPageId;
        }
    }
    SegStream::closeImpl();
}

void SegInputStream::getSegPos(SegStreamPosition &pos)
{
    CompoundId::setPageId(pos.segByteId, currPageId);
    CompoundId::setByteOffset(pos.segByteId, getBytesConsumed());
    pos.cbOffset = cbOffset;
}

void SegInputStream::seekSegPos(SegStreamPosition const &pos)
{
    assert(pageIter.isSingular());
    currPageId = CompoundId::getPageId(pos.segByteId);
    lockBuffer();
    uint cb = CompoundId::getByteOffset(pos.segByteId);
    if (cb == CompoundId::MAX_BYTE_OFFSET) {
        consumeReadPointer(getBytesAvailable());
    } else {
        consumeReadPointer(cb);
    }

    cbOffset = pos.cbOffset;
}

void SegInputStream::setDeallocate(
    bool shouldDeallocateInit)
{
    shouldDeallocate = shouldDeallocateInit;
}

bool SegInputStream::isDeallocating()
{
    return shouldDeallocate;
}

SharedByteStreamMarker SegInputStream::newMarker()
{
    return SharedByteStreamMarker(new SegStreamMarker(*this));
}

void SegInputStream::mark(ByteStreamMarker &marker)
{
    assert(&(marker.getStream()) == this);

    // memorize SegStream-specific info
    SegStreamMarker &segMarker =
        dynamic_cast<SegStreamMarker &>(marker);
    getSegPos(segMarker.segPos);
}

void SegInputStream::reset(ByteStreamMarker const &marker)
{
    assert(&(marker.getStream()) == this);

    // use SegStream-specific info
    SegStreamMarker const &segMarker =
        dynamic_cast<SegStreamMarker const &>(marker);

    // disable prefetch during seek
    bool prefetch = !pageIter.isSingular();
    endPrefetch();

    seekSegPos(segMarker.segPos);

    // restore prefetch preference
    if (prefetch) {
        startPrefetch();
    }
}

FENNEL_END_CPPFILE("$Id$");

// End SegInputStream.cpp
