/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

PageId DelegatingSegment::getPageSuccessor(PageId pageId)
{
    return pDelegateSegment->getPageSuccessor(pageId);
}

void DelegatingSegment::setPageSuccessor(PageId pageId, PageId successorId)
{
    pDelegateSegment->setPageSuccessor(pageId,successorId);
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

void DelegatingSegment::deallocatePageRange(PageId startPageId,PageId endPageId)
{
    pDelegateSegment->deallocatePageRange(startPageId,endPageId);
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
    pDelegateSegment->notifyPageDirty(page,bDataValid);
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
    pDelegateSegment->delegatedCheckpoint(delegatingSegment,checkpointType);
}

PageId DelegatingSegment::updatePage(PageId pageId, bool needsTranslation)
{
    return pDelegateSegment->updatePage(pageId, needsTranslation);
}

void DelegatingSegment::discardCachePage(BlockId blockId)
{
    return pDelegateSegment->discardCachePage(blockId);
}

FENNEL_END_CPPFILE("$Id$");

// End DelegatingSegment.cpp
