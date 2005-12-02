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

#ifndef Fennel_SegPageLock_Included
#define Fennel_SegPageLock_Included

#include "fennel/segment/Segment.h"
#include "fennel/segment/SegmentAccessor.h"
#include "fennel/cache/CacheAccessor.h"
#include "fennel/cache/CachePage.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

// TODO:  provide debug-build support for verifying that the page owner is
// correct for each page access

/**
 * A SegPageLock is associated with a single segment, and starts out in the
 * unlocked state.  It serves a function similar to a boost::scoped_lock, but
 * for locking Cache pages rather than synchronization objects.  The lockShared
 * or lockExclusive methods can be used to access a page of the segment by its
 * ID; the destructor for the SegPageLock will then unlock the page
 * automatically, unless the dontUnlock method is invoked first.
 */
class SegPageLock : public boost::noncopyable
{
    // NOTE: the shared pointers in segmentAccessor imply some locking
    // overhead during assignment.  If this is an issue, preallocate
    // the necessary SegPageLocks rather than stack-allocating them.
    
    SegmentAccessor segmentAccessor;
    CachePage *pPage;
    LockMode lockMode;

public:
    explicit SegPageLock()
    {
        pPage = NULL;
    }

    explicit SegPageLock(
        SegmentAccessor const &segmentAccessor)
    {
        pPage = NULL;
        accessSegment(segmentAccessor);
    }
    
    ~SegPageLock()
    {
        unlock();
    }

    void accessSegment(
        SegmentAccessor const &segmentAccessorInit)
    {
        assert(!pPage);
        assert(segmentAccessorInit.pSegment);
        assert(segmentAccessorInit.pCacheAccessor);
        segmentAccessor = segmentAccessorInit;
    }

    bool isLocked() const
    {
        return pPage ? true : false;
    }

    CachePage &getPage() const
    {
        assert(isLocked());
        return *pPage;
    }
    
    PageId allocatePage(PageOwnerId ownerId = ANON_PAGE_OWNER_ID)
    {
        PageId pageId = tryAllocatePage(ownerId);
        assert(pageId != NULL_PAGE_ID);
        return pageId;
    }
    
    PageId tryAllocatePage(PageOwnerId ownerId = ANON_PAGE_OWNER_ID)
    {
        unlock();
        PageId pageId = segmentAccessor.pSegment->allocatePageId(ownerId);
        if (pageId == NULL_PAGE_ID) {
            return pageId;
        }
        lockPage(pageId,LOCKMODE_X,false);
        return pageId;
    }
    
    void deallocateLockedPage()
    {
        assert(isLocked());
        BlockId blockId = pPage->getBlockId();
        unlock();
        segmentAccessor.pCacheAccessor->discardPage(blockId);
        PageId pageId = segmentAccessor.pSegment->translateBlockId(blockId);
        segmentAccessor.pSegment->deallocatePageRange(pageId,pageId);
    }

    void deallocateUnlockedPage(PageId pageId)
    {
        assert(pageId != NULL_PAGE_ID);
        BlockId blockId = segmentAccessor.pSegment->translatePageId(pageId);
        segmentAccessor.pCacheAccessor->discardPage(blockId);
        segmentAccessor.pSegment->deallocatePageRange(pageId,pageId);
    }
        
    void unlock()
    {
        if (pPage) {
            segmentAccessor.pCacheAccessor->unlockPage(*pPage,lockMode);
            pPage = NULL;
        }
    }

    void dontUnlock()
    {
        pPage = NULL;
    }
    
    void lockPage(
        PageId pageId,LockMode lockModeInit,
        bool readIfUnmapped = true)
    {
        unlock();
        lockMode = lockModeInit;
        BlockId blockId = segmentAccessor.pSegment->translatePageId(pageId);
        pPage = segmentAccessor.pCacheAccessor->lockPage(
            blockId,
            lockModeInit,
            readIfUnmapped,
            segmentAccessor.pSegment.get());
    }

    void lockPageWithCoupling(
        PageId pageId,LockMode lockModeInit)
    {
        assert(lockModeInit < LOCKMODE_S_NOWAIT);
        BlockId blockId = segmentAccessor.pSegment->translatePageId(pageId);
        CachePage *pNewPage = segmentAccessor.pCacheAccessor->lockPage(
            blockId,
            lockModeInit,
            true,
            segmentAccessor.pSegment.get());
        assert(pNewPage);
        unlock();
        lockMode = lockModeInit;
        pPage = pNewPage;
    }
    
    void lockShared(PageId pageId)
    {
        lockPage(pageId,LOCKMODE_S);
    }
    
    void lockExclusive(PageId pageId)
    {
        lockPage(pageId,LOCKMODE_X);
    }
    
    void lockSharedNoWait(PageId pageId)
    {
        lockPage(pageId,LOCKMODE_S_NOWAIT);
        lockMode = LOCKMODE_S;
    }
    
    void lockExclusiveNoWait(PageId pageId)
    {
        lockPage(pageId,LOCKMODE_X_NOWAIT);
        lockMode = LOCKMODE_X;
    }

    PageId getPageId()
    {
        return segmentAccessor.pSegment->translateBlockId(
            getPage().getBlockId());
    }

    // TODO:  big warning
    void swapBuffers(SegPageLock &other)
    {
        // TODO:  assert magic numbers the same?
        assert(isLocked());
        assert(other.isLocked());
        assert(lockMode == LOCKMODE_X);
        assert(other.lockMode == LOCKMODE_X);
        assert(pPage != other.pPage);
        // both pages will end up with this page's footer, on the assumption
        // that other was a scratch page
        // TODO:  correctly swap footers as well?
        Segment &segment = *(segmentAccessor.pSegment);
        memcpy(other.pPage->getWritableData() + segment.getUsablePageSize(),
               pPage->getReadableData() +  segment.getUsablePageSize(),
               segment.getFullPageSize() - segment.getUsablePageSize());
        pPage->swapBuffers(*other.pPage);
    }

    bool tryUpgrade()
    {
        assert(isLocked());
        assert(lockMode == LOCKMODE_S);
        if (pPage->tryUpgrade()) {
            lockMode = LOCKMODE_X;
            return true;
        }
        return false;
    }

    SharedCacheAccessor getCacheAccessor() const
    {
        return segmentAccessor.pCacheAccessor;
    }
};

/**
 * StoredNode is the base class for all structures used as headers
 * for pages of stored objects.
 *
 *<p>
 *
 * NOTE:  Great caution should be used when definining and modifying
 * stored data structures.  When defining new ones, plan ahead for future
 * changes (e.g. by defining reserved fields and filling them with reserved
 * patterns.)  Before modifying existing ones, consider whether your change
 * might invalidate extant stored databases; it may be possible to make the
 * change in a backwards-compatible fashion.
 */
struct StoredNode
{
    /**
     * Magic number identifying the derived StoredNode class.
     */
    MagicNumber magicNumber;
};

// TODO:  verify uniqueness of MAGIC_NUMBER

/**
 * SegNodeLock refines SegPageLock to allow typecasting to be hidden.  Whereas
 * a SegPageLock references an arbitrary CachePage, usage of SegNodeLock
 * demonstrates the intention to lock only pages of a given type.
 * The Node template parameter determines the derived class of StoredNode.  It
 * must have a static member MAGIC_NUMBER defining its unique magic number.
 * (To generate these, I run uuidgen on Linux and take the last 16 nybbles.)
 */
template <class Node>
class SegNodeLock : public SegPageLock
{
    void verifyMagicNumber(Node const &node) const
    {
        assert(node.magicNumber == Node::MAGIC_NUMBER);
    }
    
public:
    explicit SegNodeLock()
    {
    }
    
    explicit SegNodeLock(
        SegmentAccessor &segmentAccessor)
        : SegPageLock(segmentAccessor)
    {
    }
    
    Node const &getNodeForRead() const
    {
        Node const &node =
            *reinterpret_cast<Node const *>(getPage().getReadableData());
        verifyMagicNumber(node);
        return node;
    }
    
    Node &getNodeForWrite()
    { 
        return *reinterpret_cast<Node *>(getPage().getWritableData());
    }

    PageId allocatePage(PageOwnerId ownerId = ANON_PAGE_OWNER_ID)
    {
        PageId pageId = SegPageLock::allocatePage(ownerId);
        setMagicNumber();
        return pageId;
    }
    
    PageId tryAllocatePage(PageOwnerId ownerId = ANON_PAGE_OWNER_ID)
    {
        PageId pageId = SegPageLock::tryAllocatePage(ownerId);
        if (pageId != NULL_PAGE_ID) {
            setMagicNumber();
        }
        return pageId;
    }
    
    void setMagicNumber()
    {
        getNodeForWrite().magicNumber = Node::MAGIC_NUMBER;
    }

    bool isMagicNumberValid()
    {
        Node const &node =
            *reinterpret_cast<Node const *>(getPage().getReadableData());
        return node.magicNumber == Node::MAGIC_NUMBER;
    }
};

FENNEL_END_NAMESPACE

#endif

// End SegPageLock.h
