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
#include "fennel/segment/SegStreamAllocation.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedSegStreamAllocation SegStreamAllocation::newSegStreamAllocation()
{
    return SharedSegStreamAllocation(
        new SegStreamAllocation(),
        ClosableObjectDestructor());
}

SegStreamAllocation::SegStreamAllocation()
{
    nPagesWritten = 0;
}

void SegStreamAllocation::beginWrite(SharedSegOutputStream pSegOutputStreamInit)
{
    // go from state UNALLOCATED
    assert(!pSegOutputStream);
    assert(!pSegInputStream);

    // to state WRITING
    needsClose = true;
    pSegOutputStream = pSegOutputStreamInit;
}

void SegStreamAllocation::endWrite()
{
    // from state WRITING
    assert(pSegOutputStream);

    nPagesWritten = pSegOutputStream->getPageCount();

    SegmentAccessor segmentAccessor = pSegOutputStream->getSegmentAccessor();
    PageId firstPageId = pSegOutputStream->getFirstPageId();
    pSegOutputStream->close();
    pSegOutputStream.reset();
    if (firstPageId == NULL_PAGE_ID) {
        // go directly to UNALLOCATED
        return;
    }

    // go to state READING
    pSegInputStream = SegInputStream::newSegInputStream(
        segmentAccessor,
        firstPageId);
    pSegInputStream->setDeallocate(true);
}

SharedSegInputStream const &SegStreamAllocation::getInputStream() const
{
    return pSegInputStream;
}

void SegStreamAllocation::closeImpl()
{
    if (pSegOutputStream) {
        // state WRITING
        assert(!pSegInputStream);
        // do a fake read; this won't really read the pages, it will just
        // deallocate them
        endWrite();
    }

    if (pSegInputStream) {
        // state READING
        assert(!pSegOutputStream);
        assert(pSegInputStream->isDeallocating());
        // this will deallocate all remaining pages as a side-effect
        pSegInputStream->close();
        pSegInputStream.reset();
    } else {
        // state UNALLOCATED:  nothing to do
    }

    nPagesWritten = 0;
}

BlockNum SegStreamAllocation::getWrittenPageCount() const
{
    if (pSegOutputStream) {
        return pSegOutputStream->getPageCount();
    } else {
        return nPagesWritten;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End SegStreamAllocation.cpp
