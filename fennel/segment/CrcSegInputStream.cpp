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
#include "fennel/segment/CrcSegInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedSegInputStream CrcSegInputStream::newCrcSegInputStream(
    SegmentAccessor const &segmentAccessor,
    PseudoUuid onlineUuid,
    PageId beginPageId)
{
    return SharedSegInputStream(
        new CrcSegInputStream(
            segmentAccessor,onlineUuid,beginPageId),
        ClosableObjectDestructor());
}

CrcSegInputStream::CrcSegInputStream(
    SegmentAccessor const &segmentAccessorInit,
    PseudoUuid onlineUuidInit,
    PageId beginPageId)
    : SegInputStream(segmentAccessorInit,beginPageId,sizeof(SegStreamCrc))
{
    onlineUuid = onlineUuidInit;
}

bool CrcSegInputStream::lockBufferParanoid()
{
    if (!segmentAccessor.pSegment->isPageIdAllocated(currPageId)) {
        return false;
    }
    pageLock.lockShared(currPageId);
    if (!pageLock.getPage().isDataValid()) {
        return false;
    }
    if (!pageLock.isMagicNumberValid()) {
        return false;
    }
    SegStreamNode const &node = pageLock.getNodeForRead();
    SegStreamCrc const *pCrc =
        reinterpret_cast<SegStreamCrc const *>(&node+1);
    if (pCrc->pageId != currPageId) {
        return false;
    }
    if (pCrc->onlineUuid != onlineUuid) {
        return false;
    }
    crcComputer.reset();
    crcComputer.process_bytes(pCrc+1,node.cbData);
    if (pCrc->checksum != crcComputer.checksum()) {
        return false;
    }
    PConstBuffer pFirstByte =
        reinterpret_cast<PConstBuffer>(&node) + cbPageHeader;
    setBuffer(pFirstByte,node.cbData);
    return true;
}

void CrcSegInputStream::lockBuffer()
{
    if (!lockBufferParanoid()) {
        pageLock.unlock();
        currPageId = NULL_PAGE_ID;
        nullifyBuffer();
    }
}

FENNEL_END_CPPFILE("$Id$");

// End CrcSegInputStream.cpp
