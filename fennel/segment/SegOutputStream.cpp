/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/segment/SegOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedSegOutputStream SegOutputStream::newSegOutputStream(
    SegmentAccessor const &segmentAccessor)
{
    return SharedSegOutputStream(
        new SegOutputStream(segmentAccessor),
        ClosableObjectDestructor());
}

SegOutputStream::SegOutputStream(
    SegmentAccessor const &segmentAccessor,
    uint cbExtraHeader)
    : SegStream(segmentAccessor,cbExtraHeader)
{
    firstPageId = NULL_PAGE_ID;
    lastPageId = NULL_PAGE_ID;
    nPagesAllocated = 0;
    cbMaxPageData = getSegment()->getUsablePageSize() - cbPageHeader;
    writeLatency = WRITE_LAZY;
    // force allocation of first page
    getWritePointer(1);
}

PageId SegOutputStream::getFirstPageId() const
{
    return firstPageId;
}

BlockNum SegOutputStream::getPageCount() const
{
    return nPagesAllocated;
}

void SegOutputStream::updatePage()
{
    if (!pageLock.isLocked()) {
        return;
    }
    SegStreamNode &node = pageLock.getNodeForWrite();
    node.cbData = getBytesWrittenThisPage();
    writeExtraHeaders(node);
}

void SegOutputStream::writeExtraHeaders(SegStreamNode &)
{
}

void SegOutputStream::flushBuffer(uint cbRequested)
{
    assert(cbRequested <= cbMaxPageData);
    updatePage();
    if (pageLock.isLocked()) {
        if (writeLatency != WRITE_LAZY) {
            pageLock.getCacheAccessor()->flushPage(
                pageLock.getPage(),
                writeLatency == WRITE_EAGER_ASYNC);
        }
        pageLock.unlock();
    }
    if (!cbRequested) {
        return;
    }
    PageId pageId = pageLock.allocatePage();
    ++nPagesAllocated;
    if (firstPageId == NULL_PAGE_ID) {
        firstPageId = pageId;
    } else {
        getSegment()->setPageSuccessor(lastPageId,pageId);
    }
    lastPageId = pageId;
    SegStreamNode &node = pageLock.getNodeForWrite();
    setBuffer(
        reinterpret_cast<PBuffer>(&node)+cbPageHeader,
        cbMaxPageData);
}

void SegOutputStream::closeImpl()
{
    ByteOutputStream::closeImpl();
    SegStream::closeImpl();
}

uint SegOutputStream::getBytesWrittenThisPage() const
{
    return cbMaxPageData - getBytesAvailable();
}

void SegOutputStream::getSegPos(SegStreamPosition &pos)
{
    CompoundId::setPageId(pos.segByteId,lastPageId);
    if (getBytesAvailable()) {
        CompoundId::setByteOffset(pos.segByteId,getBytesWrittenThisPage());
    } else {
        // after a hard page break, use a special sentinel value to indicate
        // the last byte on the page
        CompoundId::setByteOffset(
            pos.segByteId,CompoundId::MAX_BYTE_OFFSET);
    }
    pos.cbOffset = cbOffset;
}

FENNEL_END_CPPFILE("$Id$");

// End SegOutputStream.cpp
