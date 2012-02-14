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
#include "fennel/segment/DynamicDelegatingSegment.h"

FENNEL_BEGIN_CPPFILE("$Id$");

DynamicDelegatingSegment::DynamicDelegatingSegment(
    WeakSegment delegatingSegment)
    : Segment(SharedSegment(delegatingSegment)->getCache()),
      delegateSegment(delegatingSegment)
{
    setUsablePageSize(SharedSegment(delegateSegment)->getUsablePageSize());
}

DynamicDelegatingSegment::~DynamicDelegatingSegment()
{
}

void DynamicDelegatingSegment::closeImpl()
{
    Segment::closeImpl();
    delegateSegment.reset();
}

BlockNum DynamicDelegatingSegment::getAllocatedSizeInPages()
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->getAllocatedSizeInPages();
}

BlockNum DynamicDelegatingSegment::getNumPagesOccupiedHighWater()
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->getNumPagesOccupiedHighWater();
}

BlockNum DynamicDelegatingSegment::getNumPagesExtended()
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->getNumPagesExtended();
}

PageId DynamicDelegatingSegment::getPageSuccessor(PageId pageId)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->getPageSuccessor(pageId);
}

void DynamicDelegatingSegment::setPageSuccessor(
    PageId pageId,
    PageId successorId)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    pDelegateSegment->setPageSuccessor(pageId, successorId);
}

BlockId DynamicDelegatingSegment::translatePageId(PageId pageId)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->translatePageId(pageId);
}

PageId DynamicDelegatingSegment::translateBlockId(BlockId blockId)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->translateBlockId(blockId);
}

PageId DynamicDelegatingSegment::allocatePageId(PageOwnerId ownerId)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->allocatePageId(ownerId);
}

bool DynamicDelegatingSegment::ensureAllocatedSize(BlockNum nPages)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->ensureAllocatedSize(nPages);
}

void DynamicDelegatingSegment::deallocatePageRange(
    PageId startPageId,
    PageId endPageId)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    pDelegateSegment->deallocatePageRange(startPageId, endPageId);
}

bool DynamicDelegatingSegment::isPageIdAllocated(PageId pageId)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->isPageIdAllocated(pageId);
}

Segment::AllocationOrder DynamicDelegatingSegment::getAllocationOrder() const
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->getAllocationOrder();
}

void DynamicDelegatingSegment::notifyPageMap(CachePage &page)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    pDelegateSegment->notifyPageMap(page);
}

void DynamicDelegatingSegment::notifyPageUnmap(CachePage &page)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    pDelegateSegment->notifyPageUnmap(page);
}

void DynamicDelegatingSegment::notifyAfterPageRead(CachePage &page)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    pDelegateSegment->notifyAfterPageRead(page);
}

void DynamicDelegatingSegment::notifyPageDirty(CachePage &page,bool bDataValid)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    pDelegateSegment->notifyPageDirty(page, bDataValid);
}

void DynamicDelegatingSegment::notifyBeforePageFlush(CachePage &page)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    pDelegateSegment->notifyBeforePageFlush(page);
}

void DynamicDelegatingSegment::notifyAfterPageFlush(CachePage &page)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    pDelegateSegment->notifyAfterPageFlush(page);
}

bool DynamicDelegatingSegment::canFlushPage(CachePage &page)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->canFlushPage(page);
}

void DynamicDelegatingSegment::delegatedCheckpoint(
    Segment &delegatingSegment,CheckpointType checkpointType)
{
    // Because the delegating segment is referenced through a weak pointer,
    // that segment may have already been freed during close database by the
    // time this method is called.  So, we need to make sure it's still
    // available before de-referencing it.
    SharedSegment pDelegateSegment = delegateSegment.lock();
    if (pDelegateSegment) {
        pDelegateSegment->delegatedCheckpoint(
            delegatingSegment,
            checkpointType);
    }
}

bool DynamicDelegatingSegment::isWriteVersioned()
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->isWriteVersioned();
}

PageId DynamicDelegatingSegment::updatePage(
    PageId pageId,
    bool needsTranslation)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->updatePage(pageId, needsTranslation);
}

MappedPageListener *DynamicDelegatingSegment::getMappedPageListener(
    BlockId blockId)
{
    // Unlike DelegatingSegment, we return the listener associated with the
    // delegating segment rather than the segment itself
    SharedSegment pDelegateSegment = delegateSegment.lock();
    return pDelegateSegment->getMappedPageListener(blockId);
}

void DynamicDelegatingSegment::setDelegatingSegment(
    WeakSegment delegatingSegment)
{
    delegateSegment = delegatingSegment;
}

SharedSegment DynamicDelegatingSegment::getDelegateSegment()
{
    return delegateSegment.lock();
}

FENNEL_END_CPPFILE("$Id$");

// End DynamicDelegatingSegment.cpp
