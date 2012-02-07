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
#include "fennel/segment/CrcSegInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedSegInputStream CrcSegInputStream::newCrcSegInputStream(
    SegmentAccessor const &segmentAccessor,
    PseudoUuid onlineUuid,
    PageId beginPageId)
{
    return SharedSegInputStream(
        new CrcSegInputStream(
            segmentAccessor, onlineUuid, beginPageId),
        ClosableObjectDestructor());
}

CrcSegInputStream::CrcSegInputStream(
    SegmentAccessor const &segmentAccessorInit,
    PseudoUuid onlineUuidInit,
    PageId beginPageId)
    : SegInputStream(segmentAccessorInit, beginPageId, sizeof(SegStreamCrc))
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
    crcComputer.process_bytes(pCrc + 1, node.cbData);
    if (pCrc->checksum != crcComputer.checksum()) {
        return false;
    }
    PConstBuffer pFirstByte =
        reinterpret_cast<PConstBuffer>(&node) + cbPageHeader;
    setBuffer(pFirstByte, node.cbData);
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
