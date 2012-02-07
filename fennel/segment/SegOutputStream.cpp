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
    : SegStream(segmentAccessor, cbExtraHeader)
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
        getSegment()->setPageSuccessor(lastPageId, pageId);
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
    CompoundId::setPageId(pos.segByteId, lastPageId);
    if (getBytesAvailable()) {
        CompoundId::setByteOffset(pos.segByteId, getBytesWrittenThisPage());
    } else {
        // after a hard page break, use a special sentinel value to indicate
        // the last byte on the page
        CompoundId::setByteOffset(
            pos.segByteId, CompoundId::MAX_BYTE_OFFSET);
    }
    pos.cbOffset = cbOffset;
}

FENNEL_END_CPPFILE("$Id$");

// End SegOutputStream.cpp
