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
#include "fennel/segment/CrcSegOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedSegOutputStream CrcSegOutputStream::newCrcSegOutputStream(
    SegmentAccessor const &segmentAccessor,
    PseudoUuid onlineUuid)
{
    return SharedSegOutputStream(
        new CrcSegOutputStream(
            segmentAccessor, onlineUuid),
        ClosableObjectDestructor());
}

CrcSegOutputStream::CrcSegOutputStream(
    SegmentAccessor const &segmentAccessorInit,
    PseudoUuid onlineUuidInit)
    : SegOutputStream(segmentAccessorInit, sizeof(SegStreamCrc))
{
    onlineUuid = onlineUuidInit;
}

void CrcSegOutputStream::writeExtraHeaders(SegStreamNode &node)
{
    SegStreamCrc *pCrc = reinterpret_cast<SegStreamCrc *>(&node+1);
    pCrc->onlineUuid = onlineUuid;
    pCrc->pageId = lastPageId;
    crcComputer.reset();
    crcComputer.process_bytes(pCrc + 1, node.cbData);
    pCrc->checksum = crcComputer.checksum();
}

FENNEL_END_CPPFILE("$Id$");

// End CrcSegOutputStream.cpp
