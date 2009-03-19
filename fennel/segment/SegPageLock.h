/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

// TODO jvs 31-Dec-2005:  Doxygen for all methods, and move inline
// bodies to end of file.

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
    PageId lockedPageId;
    bool newPage;
    bool isWriteVersioned;

    inline void resetPage()
    {
        pPage = NULL;
        lockedPageId = NULL_PAGE_ID;
        newPage = false;
    }

    inline LockMode getLockMode(LockMode origLockMode)
    {
        // If writes are versioned, then there's no need to apply an
        // exclusive lock on the current version of the page.  When
        // we need to modify the page, we'll create a new version of the
        // page, which we'll exclusively lock.
        if (isWriteVersioned) {
            if (origLockMode == LOCKMODE_X
                || origLockMode == LOCKMODE_X_NOWAIT)
            {
                return
                    (origLockMode == LOCKMODE_X) ?
                        LOCKMODE_S : LOCKMODE_S_NOWAIT;
            }
        }

        return origLockMode;
    }

    inline void initialize()
    {
        // this is a dummy to keep happy optimizing compilers which are too
        // smart for their own good
        lockMode = LOCKMODE_X;
        isWriteVersioned = false;
    }


public:
    explicit SegPageLock()
    {
        initialize();
        resetPage();
    }

    explicit SegPageLock(
        SegmentAccessor const &segmentAccessor)
    {
        initialize();
        resetPage();
        accessSegment(segmentAccessor);
    }

    ~SegPageLock()
    {
        unlock();
    }

    inline void accessSegment(
        SegmentAccessor const &segmentAccessorInit)
    {
        assert(!pPage);
        assert(segmentAccessorInit.pSegment);
        assert(segmentAccessorInit.pCacheAccessor);
        segmentAccessor = segmentAccessorInit;
        isWriteVersioned = segmentAccessor.pSegment->isWriteVersioned();
    }

    inline bool isLocked() const
    {
        return pPage ? true : false;
    }

    inline CachePage &getPage() const
    {
        assert(isLocked());
        return *pPage;
    }

    inline PageId allocatePage(PageOwnerId ownerId = ANON_PAGE_OWNER_ID)
    {
        PageId pageId = tryAllocatePage(ownerId);
        permAssert(pageId != NULL_PAGE_ID);
        return pageId;
    }

    inline PageId tryAllocatePage(PageOwnerId ownerId = ANON_PAGE_OWNER_ID)
    {
        unlock();
        PageId pageId = segmentAccessor.pSegment->allocatePageId(ownerId);
        if (pageId == NULL_PAGE_ID) {
            return pageId;
        }
        lockPage(pageId,LOCKMODE_X,false);
        newPage = true;
        return pageId;
    }

    inline void deallocateLockedPage()
    {
        assert(isLocked());
        BlockId blockId = pPage->getBlockId();
        unlock();
        PageId pageId = segmentAccessor.pSegment->translateBlockId(blockId);
        // we rely on the segment to decide whether to discard the block
        // from cache
        segmentAccessor.pSegment->deallocatePageRange(pageId,pageId);
    }

    inline void deallocateUnlockedPage(PageId pageId)
    {
        assert(pageId != NULL_PAGE_ID);
        BlockId blockId = segmentAccessor.pSegment->translatePageId(pageId);
        // we rely on the segment to decide whether to discard the block
        // from cache
        segmentAccessor.pSegment->deallocatePageRange(pageId,pageId);
    }

    inline void unlock()
    {
        if (pPage) {
            segmentAccessor.pCacheAccessor->unlockPage(
                *pPage,
                lockMode);
            resetPage();
        }
    }

    inline void dontUnlock()
    {
        resetPage();
    }

    inline void lockPage(
        PageId pageId,LockMode lockModeInit,
        bool readIfUnmapped = true)
    {
        // if the page we want to lock is already locked in the desired
        // mode, nothing needs to be done
        if (isLocked() && pageId == lockedPageId && lockMode == lockModeInit) {
            return;
        }
        unlock();
        lockMode = getLockMode(lockModeInit);
        BlockId blockId = segmentAccessor.pSegment->translatePageId(pageId);
        pPage = segmentAccessor.pCacheAccessor->lockPage(
            blockId,
            lockMode,
            readIfUnmapped,
            segmentAccessor.pSegment->getMappedPageListener(blockId));
        lockedPageId = pageId;
    }

    inline void lockPageWithCoupling(
        PageId pageId,LockMode lockModeInit)
    {
        assert(lockModeInit < LOCKMODE_S_NOWAIT);
        BlockId blockId = segmentAccessor.pSegment->translatePageId(pageId);
        LockMode newLockMode = getLockMode(lockModeInit);
        CachePage *pNewPage = segmentAccessor.pCacheAccessor->lockPage(
            blockId,
            newLockMode,
            true,
            segmentAccessor.pSegment->getMappedPageListener(blockId));
        assert(pNewPage);
        unlock();
        lockMode = newLockMode;
        pPage = pNewPage;
        lockedPageId = pageId;
    }

    inline void lockShared(PageId pageId)
    {
        lockPage(pageId,LOCKMODE_S);
    }

    inline void lockExclusive(PageId pageId)
    {
        lockPage(pageId,LOCKMODE_X);
    }

    inline void lockSharedNoWait(PageId pageId)
    {
        lockPage(pageId,LOCKMODE_S_NOWAIT);
        lockMode = LOCKMODE_S;
    }

    inline void lockExclusiveNoWait(PageId pageId)
    {
        lockPage(pageId,LOCKMODE_X_NOWAIT);
        lockMode = LOCKMODE_X;
    }

    inline void updatePage()
    {
        assert(isLocked());

        // If the page is not newly allocated and can't be updated in-place,
        // lock the page that will be updated
        if (!newPage) {
            PageId origPageId =
                segmentAccessor.pSegment->translateBlockId(getPage().
                    getBlockId());
            PageId updatePageId =
                segmentAccessor.pSegment->updatePage(origPageId);
            if (updatePageId != NULL_PAGE_ID) {
                lockUpdatePage(updatePageId);
                return;
            }
        }

        // Either the page is new or the page can be updated in-place.
        // If we haven't locked the page exclusively yet, upgrade the
        // shared lock, forcing the upgrade to wait for pending IOs.
        if (lockMode == LOCKMODE_S) {
            assert(isWriteVersioned);
            TxnId txnId = segmentAccessor.pCacheAccessor->getTxnId();
            pPage->upgrade(txnId);
            lockMode = LOCKMODE_X;
        }
    }

    inline void lockUpdatePage(PageId updatePageId)
    {
        assert(isWriteVersioned);
        BlockId blockId =
            segmentAccessor.pSegment->translatePageId(updatePageId);
        assert(lockMode == LOCKMODE_S);
        CachePage *pNewPage = segmentAccessor.pCacheAccessor->lockPage(
            blockId,
            LOCKMODE_X,
            true,
            segmentAccessor.pSegment->getMappedPageListener(blockId));
        assert(pNewPage);
        // copy the original page while we have both the original and new
        // pages locked
        memcpy(
            pNewPage->getWritableData(),
            pPage->getReadableData(),
            segmentAccessor.pSegment->getUsablePageSize());
        PageId origPageId = lockedPageId;
        unlock();
        lockMode = LOCKMODE_X;
        pPage = pNewPage;
        newPage = true;
        // keep track of the locked page based on the original pageId
        // requested
        lockedPageId = origPageId;
    }

    inline PageId getPageId()
    {
        // note that lockedPageId may not be the same as
        // segmentAccessor.pSegment->translateBlockId(getPage().getBlockId())
        // if the page is versioned
        return lockedPageId;
    }

    inline void flushPage(bool async)
    {
        assert(isLocked());
        segmentAccessor.pCacheAccessor->flushPage(getPage(), true);
    }

    // TODO:  big warning
    inline void swapBuffers(SegPageLock &other)
    {
        // TODO:  assert magic numbers the same?
        assert(isLocked());
        assert(other.isLocked());
        assert(lockMode == LOCKMODE_X);
        assert(other.lockMode == LOCKMODE_X);
        assert(pPage != other.pPage);

        // since we're copying new data into other, treat it as an update
        other.updatePage();

        // both pages will end up with this page's footer, on the assumption
        // that other was a scratch page
        // TODO:  correctly swap footers as well?
        Segment &segment = *(segmentAccessor.pSegment);
        memcpy(other.pPage->getWritableData() + segment.getUsablePageSize(),
               pPage->getReadableData() +  segment.getUsablePageSize(),
               segment.getFullPageSize() - segment.getUsablePageSize());
        pPage->swapBuffers(*other.pPage);
    }

    inline bool tryUpgrade()
    {
        assert(isLocked());
        assert(lockMode == LOCKMODE_S);
        // REVIEW jvs 31-Dec-2005:  This should really go through
        // the CacheAccessor interface.
        TxnId txnId = segmentAccessor.pCacheAccessor->getTxnId();

        // If we're versioning, defer upgrading the lock until
        // we're actually going to be update the page.
        if (isWriteVersioned) {
            return true;
        } else {
            if (pPage->tryUpgrade(txnId)) {
                lockMode = LOCKMODE_X;
                return true;
            }
            return false;
        }
    }

    inline SharedCacheAccessor getCacheAccessor() const
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
 *
 *<p>
 *
 * For more information, see <a
 * href="http://pub.eigenbase.org/wiki/FennelPageBasedDataStructureHowto">the
 * HOWTO</a>.
 */
template <class Node>
class SegNodeLock : public SegPageLock
{
    inline void verifyMagicNumber(Node const &node) const
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

    inline bool checkMagicNumber() const
    {
        Node const &node =
            *reinterpret_cast<Node const *>(getPage().getReadableData());
        return (node.magicNumber == Node::MAGIC_NUMBER);
    }

    inline Node const &getNodeForRead() const
    {
        Node const &node =
            *reinterpret_cast<Node const *>(getPage().getReadableData());
        verifyMagicNumber(node);
        return node;
    }

    inline Node &getNodeForWrite()
    {
        updatePage();
        return *reinterpret_cast<Node *>(getPage().getWritableData());
    }

    inline PageId allocatePage(PageOwnerId ownerId = ANON_PAGE_OWNER_ID)
    {
        PageId pageId = SegPageLock::allocatePage(ownerId);
        setMagicNumber();
        return pageId;
    }

    inline PageId tryAllocatePage(PageOwnerId ownerId = ANON_PAGE_OWNER_ID)
    {
        PageId pageId = SegPageLock::tryAllocatePage(ownerId);
        if (pageId != NULL_PAGE_ID) {
            setMagicNumber();
        }
        return pageId;
    }

    inline void setMagicNumber()
    {
        getNodeForWrite().magicNumber = Node::MAGIC_NUMBER;
    }

    inline bool isMagicNumberValid()
    {
        Node const &node =
            *reinterpret_cast<Node const *>(getPage().getReadableData());
        return node.magicNumber == Node::MAGIC_NUMBER;
    }
};

FENNEL_END_NAMESPACE

#endif

// End SegPageLock.h
