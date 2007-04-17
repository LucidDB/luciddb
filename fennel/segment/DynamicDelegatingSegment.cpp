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
    pDelegateSegment->setPageSuccessor(pageId,successorId);
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
    pDelegateSegment->deallocatePageRange(startPageId,endPageId);
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
    pDelegateSegment->notifyPageDirty(page,bDataValid);
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

void DynamicDelegatingSegment::discardCachePage(BlockId blockId)
{
    SharedSegment pDelegateSegment = delegateSegment.lock();
    pDelegateSegment->discardCachePage(blockId);
}

FENNEL_END_CPPFILE("$Id$");

// End DynamicDelegatingSegment.cpp
