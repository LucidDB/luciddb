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
#include "fennel/segment/SegStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SegStream::SegStream(
    SegmentAccessor const &segmentAccessorInit,uint cbExtraHeader)
    : segmentAccessor(segmentAccessorInit),
      pageLock(segmentAccessor)
{
    cbPageHeader = sizeof(SegStreamNode) + cbExtraHeader;
}

void SegStream::closeImpl()
{
    pageLock.unlock();
    segmentAccessor.reset();
}

SharedSegment SegStream::getSegment() const
{
    return segmentAccessor.pSegment;
}

SegmentAccessor const &SegStream::getSegmentAccessor() const
{
    return segmentAccessor;
}

SegStreamMarker::SegStreamMarker(SegStream const &segStream)
    : ByteStreamMarker(segStream)
{
    segPos.segByteId = SegByteId(MAXU);
    segPos.cbOffset = MAXU;
}

FileSize SegStreamMarker::getOffset() const
{
    return segPos.cbOffset;
}

FENNEL_END_CPPFILE("$Id$");

// End SegStream.cpp
