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
#include "fennel/segment/DelegatingSegment.h"

FENNEL_BEGIN_CPPFILE("$Id$");

DelegatingSegment::DelegatingSegment(
    SharedSegment pDelegateSegmentInit)
    : Segment(pDelegateSegmentInit->getCache()),
      pDelegateSegment(pDelegateSegmentInit)
{
    setUsablePageSize(pDelegateSegment->getUsablePageSize());
}

DelegatingSegment::~DelegatingSegment()
{
}

void DelegatingSegment::closeImpl()
{
    Segment::closeImpl();
    pDelegateSegment.reset();
}

BlockNum DelegatingSegment::getAllocatedSizeInPages()
{
    return pDelegateSegment->getAllocatedSizeInPages();
}

BlockNum DelegatingSegment::getNumPagesOccupiedHighWater()
{
    return pDelegateSegment->getNumPagesOccupiedHighWater();
}

BlockNum DelegatingSegment::getNumPagesExtended()
{
    return pDelegateSegment->getNumPagesExtended();
}

PageId DelegatingSegment::getPageSuccessor(PageId pageId)
{
    return pDelegateSegment->getPageSuccessor(pageId);
}

void DelegatingSegment::setPageSuccessor(PageId pageId, PageId successorId)
{
    pDelegateSegment->setPageSuccessor(pageId, successorId);
}

BlockId DelegatingSegment::translatePageId(PageId pageId)
{
    return pDelegateSegment->translatePageId(pageId);
}

PageId DelegatingSegment::translateBlockId(BlockId blockId)
{
    return pDelegateSegment->translateBlockId(blockId);
}

PageId DelegatingSegment::allocatePageId(PageOwnerId ownerId)
{
    return pDelegateSegment->allocatePageId(ownerId);
}

bool DelegatingSegment::ensureAllocatedSize(BlockNum nPages)
{
    return pDelegateSegment->ensureAllocatedSize(nPages);
}

void DelegatingSegment::deallocatePageRange(
    PageId startPageId, PageId endPageId)
{
    pDelegateSegment->deallocatePageRange(startPageId, endPageId);
}

bool DelegatingSegment::isPageIdAllocated(PageId pageId)
{
    return pDelegateSegment->isPageIdAllocated(pageId);
}

Segment::AllocationOrder DelegatingSegment::getAllocationOrder() const
{
    return pDelegateSegment->getAllocationOrder();
}

void DelegatingSegment::notifyPageMap(CachePage &page)
{
    pDelegateSegment->notifyPageMap(page);
}

void DelegatingSegment::notifyPageUnmap(CachePage &page)
{
    pDelegateSegment->notifyPageUnmap(page);
}

void DelegatingSegment::notifyAfterPageRead(CachePage &page)
{
    pDelegateSegment->notifyAfterPageRead(page);
}

void DelegatingSegment::notifyPageDirty(CachePage &page,bool bDataValid)
{
    pDelegateSegment->notifyPageDirty(page, bDataValid);
}

void DelegatingSegment::notifyBeforePageFlush(CachePage &page)
{
    pDelegateSegment->notifyBeforePageFlush(page);
}

void DelegatingSegment::notifyAfterPageFlush(CachePage &page)
{
    pDelegateSegment->notifyAfterPageFlush(page);
}

bool DelegatingSegment::canFlushPage(CachePage &page)
{
    return pDelegateSegment->canFlushPage(page);
}

void DelegatingSegment::delegatedCheckpoint(
    Segment &delegatingSegment,CheckpointType checkpointType)
{
    pDelegateSegment->delegatedCheckpoint(delegatingSegment, checkpointType);
}

PageId DelegatingSegment::updatePage(PageId pageId, bool needsTranslation)
{
    return pDelegateSegment->updatePage(pageId, needsTranslation);
}

FENNEL_END_CPPFILE("$Id$");

// End DelegatingSegment.cpp
