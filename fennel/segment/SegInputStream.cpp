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
#include "fennel/segment/SegInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedSegInputStream SegInputStream::newSegInputStream(
    SegmentAccessor const &segmentAccessor,
    PageId beginPageId)
{
    return SharedSegInputStream(
        new SegInputStream(segmentAccessor,beginPageId),
        ClosableObjectDestructor());
}

SegInputStream::SegInputStream(
    SegmentAccessor const &segmentAccessor,
    PageId beginPageId,
    uint cbExtraHeader)
    : SegStream(segmentAccessor,cbExtraHeader)
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
    pageIter.mapRange(segmentAccessor,currPageId);
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
    setBuffer(pFirstByte,node.cbData);
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
    CompoundId::setPageId(pos.segByteId,currPageId);
    CompoundId::setByteOffset(pos.segByteId,getBytesConsumed());
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
