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

#ifndef Fennel_TwoQVictimPolicy_Included
#define Fennel_TwoQVictimPolicy_Included

#include "fennel/common/IntrusiveDList.h"

#include <boost/dynamic_bitset.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * TwoQPageQueue is used to implement the page queues used by the
 * TwoQVictimPolicy.  It can be used to implement LRU and FIFO queues,
 * based on how the caller inserts and moves elements in the queue.
 */
class TwoQPageQueue
{
    /**
     * Head of the queue
     */
    IntrusiveDListNode *head;

    /**
     * Tail of the queue
     */
    IntrusiveDListNode *tail;

    /**
     * Length of the queue
     */
    uint len;

    /**
     * Validates the contents of the queue.  Used for debugging.
     */
    void validateQueue()
    {
        assert(head == NULL || head->getPrev() == NULL);
        assert(tail == NULL || tail->getNext() == NULL);

        uint actualLen = 0;
        IntrusiveDListNode *curr = head;
        while (curr != NULL) {
            actualLen++;
            curr = curr->getNext();
        }
        assert(actualLen == len);

        curr = tail;
        actualLen = 0;
        while (curr != NULL) {
            actualLen++;
            curr = curr->getPrev();
        }
        assert(actualLen == len);
    }

public:
    TwoQPageQueue()
    {
        head = NULL;
        tail = NULL;
        len = 0;
    }

    /**
     * Inserts a page at the head of a queue.
     *
     * @param page the page being inserted
     */
    void insertAtHead(IntrusiveDListNode &page)
    {
        if (head == NULL) {
            assert(tail == NULL);
            tail = &page;
        } else {
            page.insertBefore(*head);
        }
        head = &page;
        len++;
    }

    /**
     * Moves a page from its current position to the head of the queue.
     *
     * @param page the page being moved
     */
    void moveToHead(IntrusiveDListNode &page)
    {
        if (head == &page) {
            return;
        }
        if (tail == &page) {
            tail = page.getPrev();
        }
        page.detach();
        page.insertBefore(*head);
        head = &page;
    }

    /**
     * Inserts a page at the tail of the queue.
     *
     * @param page the page being inserted
     */
    void insertAtTail(IntrusiveDListNode &page)
    {
        if (tail == NULL) {
            assert(head == NULL);
            head = &page;
        } else {
            page.insertAfter(*tail);
        }
        tail = &page;
        len++;
    }

    /**
     * Moves a page from its current position to the tail of the queue.
     *
     * @param page the page being moved
     */
    void moveToTail(IntrusiveDListNode &page)
    {
        if (tail == &page) {
            return;
        }
        if (head == &page) {
            head = page.getNext();
        }
        page.detach();
        page.insertAfter(*tail);
        tail = &page;
    }

    /**
     * Removes a page from the queue.
     *
     * @param page the page being removed
     */
    void remove(IntrusiveDListNode &page)
    {
        assert(len == 1 || (page.getNext() != NULL || page.getPrev() != NULL));
        if (&page == head && &page == tail) {
            assert(len == 1);
            head = tail = NULL;
        } else if (&page == head) {
            head = head->getNext();
        } else if (&page == tail) {
            tail = tail->getPrev();
        }
        page.detach();
        assert(len > 0);
        len--;
    }

    /**
     * @returns size of the queue
     */
    uint size()
    {
        return len;
    }

    /**
     * @return a pointer to the head of the queue; NULL if the queue is empty
     */
    IntrusiveDListNode *getHead()
    {
        return head;
    }
};

/**
 * TwoQDirtyPage is the attribute class that contains information about dirty
 * pages.  Objects representing dirty pages are linked together in
 * doubly-linked lists with back pointers to the dirty parent pages.  The
 * lists are subsets of the full 2Q page queues, as they consist of only the
 * dirty pages.
 */
class TwoQDirtyPage : public IntrusiveDListNode
{
public:
    /**
     * Enumeration indicating whether a page is dirty and if so, which
     * queue the dirty page is in
     */
    enum DirtyPageState {
        /**
         * Page is not dirty
         */
        PAGE_CLEAN,

        /**
         * Page is dirty and in the popular-unpinned queue
         */
        PAGE_DIRTY_POPULAR_UNPINNED,

        /**
         * Page is dirty and in the freshmen queue
         */
        PAGE_DIRTY_FRESHMAN,

        /**
         * Page is dirty and currently in the popular-pinned queue
         */
        PAGE_DIRTY_POPULAR_PINNED
    };

private:
    /**
     * Pointer to the parent page corresponding to this dirty object
     */
    CachePage *pParentPage;

    /**
     * The state of the dirty page
     */
    DirtyPageState dirtyState;

public:
    /**
     * Sets the parent page corresponding to the dirty object.
     *
     * @param parentPageInit the parent page
     */
    void setParentPage(CachePage &parentPageInit)
    {
        pParentPage = &parentPageInit;
    }

    /**
     * @return the parent page corresponding to the dirty object
     */
    CachePage &getParentPage()
    {
        return *pParentPage;
    }

    /**
     * @return dirty state of a page
     */
    DirtyPageState getDirtyState()
    {
        return dirtyState;
    }

    /**
     * Sets a page's dirty state.
     *
     * @param newState the new dirty state value for a page
     */
    void setDirtyState(DirtyPageState newState)
    {
        dirtyState = newState;
    }
};

/**
 * TwoQVictim is the attributes class which must be a base for any CachePage
 * type which will be cached using a TwoQVictimPolicy.
 */
class TwoQVictim : public IntrusiveDListNode
{
public:
    /**
     * Enumeration of possible state values for a page when used with
     * TwoQVictimPolicy
     */
    enum PageState {
        /**
         * Page is in the popular-pinned queue
         */
        PAGE_STATE_POPULAR_PINNED,
        /**
         * Page is in the popular-unpinned queue
         */
        PAGE_STATE_POPULAR_UNPINNED,
        /**
         * Page is in the freshmen queue
         */
        PAGE_STATE_FRESHMAN,
        /**
         * Page is not currently in the cache
         */
        PAGE_STATE_FREE
    };

private:
    /**
     * State of the page
     */
    PageState state;

    /**
     * Contains information about dirty pages
     */
    TwoQDirtyPage dirtyPageNode;

public:

    TwoQVictim()
    {
        state = PAGE_STATE_FREE;
        dirtyPageNode.setDirtyState(TwoQDirtyPage::PAGE_CLEAN);
    }

    /**
     * @return state of a page
     */
    PageState getState()
    {
        return state;
    }

    /**
     * Sets a page's state
     *
     * @param newState the new state value for a page
     */
    void setState(PageState newState)
    {
        state = newState;
    }

    /**
     * @return the dirty node corresponding to the page
     */
    TwoQDirtyPage &getDirtyPageNode()
    {
        return dirtyPageNode;
    }
};

/**
 * TwoQPageListIter iterates over queues containing pages.
 */
template <class PageT>
class TwoQPageListIter : public IntrusiveTwoDListIter<PageT, PageT>
{
    /**
     * @return the page itself
     */
    virtual PageT *getReturnElement(PageT *page) const
    {
        return page;
    }

public:
    TwoQPageListIter()
        : IntrusiveTwoDListIter<PageT, PageT>()
    {
    }

    TwoQPageListIter(PageT *list1, PageT *list2)
        : IntrusiveTwoDListIter<PageT, PageT>(list1, list2)
    {
    }

    ~TwoQPageListIter()
    {
    }
};


/**
 * TwoQDirtyPageListIter iterates over queues containing dirty nodes,
 * returning the parent pages corresponding to the dirty nodes.
 */
template <class PageT>
class TwoQDirtyPageListIter : public IntrusiveTwoDListIter<TwoQDirtyPage, PageT>
{
    /**
     * Returns a pointer to the parent cache page corresponding to the
     * dirty node that the iterator is currently positioned on.
     *
     * @param dirtyPage the dirty page node
     *
     * @return pointer to the cache page corresponding to the current dirty
     * page
     */
    virtual PageT *getReturnElement(TwoQDirtyPage *dirtyPage) const
    {
        if (dirtyPage == NULL) {
            return NULL;
        } else {
            return static_cast<PageT *>(&(dirtyPage->getParentPage()));
        }
    }

public:
    TwoQDirtyPageListIter()
        : IntrusiveTwoDListIter<TwoQDirtyPage, PageT>()
    {
    }

    TwoQDirtyPageListIter(TwoQDirtyPage *list1, TwoQDirtyPage *list2)
        : IntrusiveTwoDListIter<TwoQDirtyPage, PageT>(list1, list2)
    {
    }

    ~TwoQDirtyPageListIter()
    {
    }
};

/**
 * TwoQVictimPolicy implements the 2Q page victimization policy as described
 * in the VLDB '94 paper "2Q: A Low Overhead High Performance Buffer Management
 * Replacement Algorithm" by Johnson and Shasha.
 *
 * <p>
 * The 2Q refers to the fact that the algorithm separates the pages in the
 * cache into two separate queues -- a LRU popular queue for frequently
 * accessed pages and a FIFO freshmen queue for less frequently accessed
 * pages.  There is also a FIFO history queue that keeps track of pages that
 * have been victimized from the freshmen queue.  That history queue is
 * what's used to determine whether a page is a popular one.  In other words,
 * when a page is first added to the cache, it's added to the freshmen queue.
 * Once it's victimized from the freshmen queue, it's added to the history
 * queue.  If a page is re-referenced while it's in the history queue, it's
 * added to the popular queue.  By keeping two separate queues, the algorithm
 * is scan resistant.  Therefore, if you are doing large sequential scans,
 * the scan resistant property prevents the scan pages from flooding the
 * cache, paging out index pages that are more commonly referenced,
 * particularly, index root pages. Ideally, you want to keep index root pages
 * cached and page out the scan pages instead, even if the latter was more
 * recently referenced.
 *
 * <p>
 * Note that the naming of the queues is different from the less intuitive
 * terms used in the original Johnson/Shasha paper -- Am (popular), A1in
 * (freshmen), and A1out (history).  Also, the popular queue is divided into
 * two separate queues -- pinned vs unpinned pages.  This makes locating a
 * page for victimization more efficient.
 *
 * <p>
 * One other extension in our implementation is we maintain separate queues
 * of dirty pages for pages currently in the freshmen and popular-unpinned
 * queues.  These queues are subsets of their "parent" queues.  By separating
 * out dirty pages, this makes locating candidate pages for flushing
 * more efficient when you have a large number of cache pages, many of which
 * aren't dirty.
 *
 * <p>
 * Any realization for PageT must inherit both CachePage and TwoQVictim as
 * bases.
 */
template <class PageT>
class TwoQVictimPolicy
{
    /**
     * SXMutex protecting the queues
     */
    SXMutex mutex;

    /**
     * FIFO queue of popular, pinned pages.  No need to make it LRU since
     * pages from this queue can't be victimized.
     */
    TwoQPageQueue popularPinnedQueue;

    /**
     * LRU queue of popular, unpinned pages
     */
    TwoQPageQueue popularUnpinnedQueue;

    /**
     * FIFO queue of freshmen pages
     */
    TwoQPageQueue freshmenQueue;

    /**
     * Companion queue to popularUnpinnedQueue, except it only includes
     * dirty pages and contains pointers to TwoQDirtyPage objects
     */
    TwoQPageQueue dirtyPopularUnpinnedQueue;

    /**
     * Companion queue to freshmenQueue, except it only includes dirty pages
     * and contains pointers to TwoQDirtyPage objects
     */
    TwoQPageQueue dirtyFreshmenQueue;

    /**
     * FIFO history queue
     */
    std::vector<BlockId> historyQueue;

    /**
     * Index corresponding to the first element in the historyQueue
     */
    uint historyQueueStart;

    /**
     * Current length of the historyQueue
     */
    uint currHistoryQueueLen;

    /**
     * Bitmap used to quickly approximate whether a page exists in the history
     * queue based on the page's BlockId.  The determination is approximate
     * because multiple BlockIds can map to the same position in the bitmap.
     * However, the likelihood of those collisions occuring should be small,
     * given the size of the bitmap.
     */
    boost::dynamic_bitset<> historyBitmap;

    /**
     * Total number of buffer pages in the cache
     */
    uint nCachePages;

    /**
     * Maximum number of pages in the freshmen queue.  This number can be
     * exceeded if there are not enough popular pages, or freshmen pages can't
     * be victimized because they're all pinned.
     */
    uint maxFreshmenQueueLen;

    /**
     * Maximum number of pages in the history queue
     */
    uint maxHistoryQueueLen;

    /**
     * Percentage of the total cache set aside for the freshmen queue.  Note
     * that this is a "soft" percentage, and the number of pages in the
     * freshmen queue can exceed this number.
     */
    uint freshmenQueuePercentage;

    /**
     * The percentage of the total number of cache pages that dictates the
     * number of pages in this history queue
     */
    uint pageHistoryQueuePercentage;

    /**
     * Initializes various queue variables.
     */
    void init()
    {
        nCachePages = 0;
        maxFreshmenQueueLen = 0;
        maxHistoryQueueLen = 0;
        historyQueueStart = 0;
        currHistoryQueueLen = 0;
    }

    /**
     * @return true if a page's state is non-dirty and its dirty state is clean
     */
    bool isPageClean(PageT &page)
    {
        return
            (!page.isDirty() &&
                page.getDirtyPageNode().getDirtyState() ==
                    TwoQDirtyPage::PAGE_CLEAN);
    }

    /**
     * Notifies the policy that a page that's being accessed needs to be put
     * into the popular queue.  Depending on the "pin" parameter, it's either
     * put into the popular-pinned queue or the popular-unpinned queue.
     *
     * @param page the page being accessed
     * @param pin whether the page is being pinned
     */
    void notifyPopularPageAccess(PageT &page, bool pin)
    {
        assert(mutex.isLocked(LOCKMODE_X));
        TwoQVictim::PageState state = page.TwoQVictim::getState();
        assert(
            state != TwoQVictim::PAGE_STATE_FRESHMAN &&
            state != TwoQVictim::PAGE_STATE_POPULAR_PINNED);

        if (pin) {
            // Remove the page from the popular-unpinned queues and add
            // it to the popular-pinned queue.
            if (state == TwoQVictim::PAGE_STATE_POPULAR_UNPINNED) {
                popularUnpinnedQueue.remove(page);
                TwoQDirtyPage &dirtyPageNode =
                    page.TwoQVictim::getDirtyPageNode();
                if (dirtyPageNode.getDirtyState() ==
                    TwoQDirtyPage::PAGE_DIRTY_POPULAR_UNPINNED)
                {
                    dirtyPopularUnpinnedQueue.remove(dirtyPageNode);
                    dirtyPageNode.setDirtyState(
                        TwoQDirtyPage::PAGE_DIRTY_POPULAR_PINNED);
                } else {
                    assert(isPageClean(page));
                }
            }
            popularPinnedQueue.insertAtTail(page);
            page.TwoQVictim::setState(TwoQVictim::PAGE_STATE_POPULAR_PINNED);

        } else {
            // Move the page to the end of the popular-unpinned queues.
            if (state == TwoQVictim::PAGE_STATE_POPULAR_UNPINNED) {
                popularUnpinnedQueue.moveToTail(page);
                TwoQDirtyPage &dirtyPageNode =
                    page.TwoQVictim::getDirtyPageNode();
                if (dirtyPageNode.getDirtyState() ==
                    TwoQDirtyPage::PAGE_DIRTY_POPULAR_UNPINNED)
                {
                    dirtyPopularUnpinnedQueue.moveToTail(dirtyPageNode);
                } else {
                    assert(isPageClean(page));
                }
            } else {
                // The page was originally free and now needs to be put
                // into the popular-unpinned queue.
                assert(isPageClean(page));
                popularUnpinnedQueue.insertAtTail(page);
                page.TwoQVictim::setState(
                    TwoQVictim::PAGE_STATE_POPULAR_UNPINNED);
            }
        }
    }

public:

    /**
     * All models for VictimPolicy must define a nested public type
     * PageIterator, which is used by CacheImpl to iterate over candidate
     * victims in optimal order.  This type must be a model for a standard
     * forward iterator.  For TwoQVictimPolicy, this is accomplished by
     * iterating over two doubly-linked list of pages corresponding to
     * queues, one after the other.
     */
    typedef TwoQPageListIter<PageT> PageIterator;

    /**
     * All models for VictimPolicy must define a nested public type
     * DirtyPageIterator, which is used by CacheImpl to iterate over pages
     * that can potentially be flushed by the lazy page writer that runs
     * in a background thread.  In the case of TwoQVictimPolicy, this iterator
     * only returns dirty pages, avoiding clean pages.
     */
    typedef TwoQDirtyPageListIter<PageT> DirtyPageIterator;

    /**
     * All models for VictimPolicy must define a nested public type SharedGuard,
     * which is held by CacheImpl while iterating over candidate victims.
     * This guard must provide any synchronization required to protect
     * the iteration against concurrent modifications.  The guard will be
     * initialized with the result of getMutex().  For TwoQVictimPolicy, the
     * guard is a read guard protecting the various queues, meaning
     * notifications are blocked during iteration since they have to
     * modify the chain.
     */
    typedef SXMutexSharedGuard SharedGuard;

    /**
     * Guard for write access to mutex.
     */
    typedef SXMutexExclusiveGuard ExclusiveGuard;

    /**
     * All models for VictimPolicy must have a default constructor.
     */
    TwoQVictimPolicy()
    {
        init();
        freshmenQueuePercentage = CacheParams::defaultFreshmenQueuePercentage;
        pageHistoryQueuePercentage =
            CacheParams::defaultPageHistoryQueuePercentage;
    }

    TwoQVictimPolicy(const CacheParams &params)
    {
        init();
        freshmenQueuePercentage = params.freshmenQueuePercentage;
        pageHistoryQueuePercentage = params.pageHistoryQueuePercentage;
    }

    /**
     * Receives notification from CacheImpl, indicating the total number of
     * buffer pages in the cache.
     *
     * @param nCachePagesInit number of buffer pages in the cache
     */
    void setAllocatedPageCount(uint nCachePagesInit)
    {
        ExclusiveGuard exclusiveGuard(mutex);

        if (nCachePagesInit == nCachePages) {
            return;
        }

        uint newHistoryQueueLen =
            nCachePagesInit * pageHistoryQueuePercentage / 100;
        if ((currHistoryQueueLen > newHistoryQueueLen) ||
            (historyQueueStart != 0))
        {
            // If the new queue is smaller than the currently used size,
            // copy the existing history queue into a temporary vector,
            // truncating the front of the queue to match the size of
            // the new vector.  We also need to recreate the vector if
            // the new queue is larger than the currently used size,
            // and the existing history queue has cycled around such
            // that the starting element is no longer at position 0.
            std::vector<BlockId> temp;
            temp.resize(newHistoryQueueLen);
            uint excess =
                (currHistoryQueueLen > newHistoryQueueLen)
                    ? (currHistoryQueueLen - newHistoryQueueLen) : 0;
            currHistoryQueueLen -= excess;
            for (uint i = 0; i < currHistoryQueueLen; i++) {
                temp[i] =
                    historyQueue[
                        (historyQueueStart + excess + i) %
                        maxHistoryQueueLen];
            }
            historyQueueStart = 0;
            historyQueue.resize(newHistoryQueueLen);
            historyQueue.swap(temp);
        } else {
            historyQueue.resize(newHistoryQueueLen);
        }

        // It's possible that a blockId has been removed from the history
        // bitmap, but the corresponding update hasn't been made in the
        // history queue itself.  If so, remove the blockId from the queue
        // so when we rebuild the bitmap below, that blockId won't be included.
        assert(historyQueueStart == 0);
        for (uint i = 0; i < currHistoryQueueLen; i++) {
            if (!historyBitmap.test(
                opaqueToInt(historyQueue[i] % historyBitmap.size())))
            {
                historyQueue[i] = NULL_BLOCK_ID;
            }
        }

        nCachePages = nCachePagesInit;
        maxFreshmenQueueLen = nCachePages * freshmenQueuePercentage / 100;
        maxHistoryQueueLen = newHistoryQueueLen;

        historyBitmap.resize(maxHistoryQueueLen * 64);
        // Rebuild the history bitmap, if there are existing entries in the
        // history queue.  Ignore the invalid blockIds.
        if (currHistoryQueueLen > 0) {
            historyBitmap.reset();
            for (uint i = 0; i < currHistoryQueueLen; i++) {
                if (historyQueue[i] != NULL_BLOCK_ID) {
                    historyBitmap.set(
                        opaqueToInt(historyQueue[i]) % historyBitmap.size());
                }
            }
        }
    }

    /**
     * Receives notification from CacheImpl when a page is allocated,
     * giving the VictimPolicy a chance to initialize its own data structures
     * for this page.
     *
     * @param page the page being allocated
     */
    void registerPage(PageT &page)
    {
        page.TwoQVictim::setState(TwoQVictim::PAGE_STATE_FREE);
        page.TwoQVictim::getDirtyPageNode().setParentPage(page);
        page.TwoQVictim::getDirtyPageNode().setDirtyState(
            TwoQDirtyPage::PAGE_CLEAN);
    }

    /**
     * Receives notification from CacheImpl when a page is freed, giving the
     * VictimPolicy a chance to deinitialize its own data structures for this
     * page.
     *
     * @param page the page being freed
     */
    void unregisterPage(PageT &page)
    {
        assert(page.TwoQVictim::getState() == TwoQVictim::PAGE_STATE_FREE);
        assert(
            page.TwoQVictim::getDirtyPageNode().getDirtyState() ==
            TwoQDirtyPage::PAGE_CLEAN);
    }

    /**
     * Receives notification from CacheImpl when a page is accessed.  On entry,
     * the page's mutex is held by the calling thread, so the state of the page
     * (e.g. its mapped BlockId) is guaranteed to remain stable.  This is true
     * for all other notify methods as well.  For TwoQVictimPolicy, depending
     * on the current state of the page, that determines what happens to the
     * page.
     *
     * <ul>
     * <li>If the page is currently either in the freshmen or popular-pinned
     * queues, then the page remains in its current position in the queue.
     *
     * <li>If the page is currently in the popular-unpinned queue and it's
     * being pinned, then it's added to the popular-pinned queue.  Otherwise,
     * if it's not being pinned, it's added to the tail of the popular-unpinned
     * queue.
     *
     * <li>If the page is not currently in the cache and it's in the history
     * queue, it's added to one of the popular queues, depending on whether
     * it's being pinned.  Otherwise, if it's not in the history queue, then
     * it's added to the tail of the freshmen queue.
     * </ul>
     *
     * @param page the page being accessed
     * @param pin if true, the page being accessed will be pinned in the cache
     */
    void notifyPageAccess(PageT &page, bool pin)
    {
        ExclusiveGuard exclusiveGuard(mutex);

        switch (page.TwoQVictim::getState()) {
        case TwoQVictim::PAGE_STATE_FRESHMAN:
        case TwoQVictim::PAGE_STATE_POPULAR_PINNED:
            return;

        case TwoQVictim::PAGE_STATE_POPULAR_UNPINNED:
            notifyPopularPageAccess(page, pin);
            break;

        case TwoQVictim::PAGE_STATE_FREE:
            if (historyBitmap.test(
                opaqueToInt(page.getBlockId()) % historyBitmap.size()))
            {
                notifyPopularPageAccess(page, pin);
            } else {
                assert(isPageClean(page));
                page.TwoQVictim::setState(TwoQVictim::PAGE_STATE_FRESHMAN);
                freshmenQueue.insertAtTail(page);
            }
            break;

        default:
            permAssert(false);
        }
    }

    /**
     * Receives notification from CacheImpl on a hint that a page
     * is a good candidate for victimization.  For TwoQVictimPolicy, this
     * results in the page being moved to the head of the queue if the page
     * is in either the popular-unpinned or freshmen queues.
     *
     * @param page the page to which the hint pertains
     */
    void notifyPageNice(PageT &page)
    {
        ExclusiveGuard exclusiveGuard(mutex);
        TwoQVictim::PageState state = page.TwoQVictim::getState();
        assert(state != TwoQVictim::PAGE_STATE_FREE);

        if (state == TwoQVictim::PAGE_STATE_POPULAR_UNPINNED) {
            popularUnpinnedQueue.moveToHead(page);
            TwoQDirtyPage &dirtyPageNode = page.TwoQVictim::getDirtyPageNode();
            if (dirtyPageNode.getDirtyState() ==
                TwoQDirtyPage::PAGE_DIRTY_POPULAR_UNPINNED)
            {
                dirtyPopularUnpinnedQueue.moveToHead(dirtyPageNode);
            } else {
                assert(isPageClean(page));
            }
        } else if (state == TwoQVictim::PAGE_STATE_FRESHMAN) {
            freshmenQueue.moveToHead(page);
            TwoQDirtyPage &dirtyPageNode = page.TwoQVictim::getDirtyPageNode();
            if (dirtyPageNode.getDirtyState() ==
                TwoQDirtyPage::PAGE_DIRTY_FRESHMAN)
            {
                dirtyFreshmenQueue.moveToHead(dirtyPageNode);
            } else {
                assert(isPageClean(page));
            }
        }
    }

    /**
     * Receives notification from CacheImpl just after a page is mapped.  This
     * implies an access as well, so for efficiency, no corresponding
     * notifyPageAccess notification is received.
     *
     * @param page the page being mapped
     * @param pin if true, the page being mapped will be pinned in the cache
     */
    void notifyPageMap(PageT &page, bool pin)
    {
        // first access for a newly mapped page will not get a corresponding
        // call to notifyPageAcces, so do it now
        notifyPageAccess(page, pin);
    }

    /**
     * Receives notification from CacheImpl just before a page is unmapped.
     * The Page object still has the ID being unmapped.
     *
     * @param page the page being unmapped
     *
     * @param discard if true, page is being discarded from the cache
     */
    void notifyPageUnmap(PageT &page, bool discard)
    {
        ExclusiveGuard exclusiveGuard(mutex);
        TwoQVictim::PageState state = page.TwoQVictim::getState();
        assert(
            state != TwoQVictim::PAGE_STATE_POPULAR_PINNED &&
            state != TwoQVictim::PAGE_STATE_FREE);

        if (state == TwoQVictim::PAGE_STATE_POPULAR_UNPINNED) {
            popularUnpinnedQueue.remove(page);
            TwoQDirtyPage &dirtyPageNode =
                page.TwoQVictim::getDirtyPageNode();
            if (dirtyPageNode.getDirtyState() ==
                TwoQDirtyPage::PAGE_DIRTY_POPULAR_UNPINNED)
            {
                dirtyPopularUnpinnedQueue.remove(dirtyPageNode);
                dirtyPageNode.setDirtyState(TwoQDirtyPage::PAGE_CLEAN);
            } else {
                assert(isPageClean(page));
            }
        } else {
            // If the page is currently in the freshmen queue, add it to
            // the history queue, unless the page is being discarded.
            // Remove the page currently at the head of the history queue,
            // if the history queue is at max capacity.  Also make the
            // corresponding updates in the history bitmap.
            if (!discard) {
                if (currHistoryQueueLen >= maxHistoryQueueLen) {
                    historyBitmap.set(
                        opaqueToInt(historyQueue[historyQueueStart])
                            % historyBitmap.size(),
                        false);
                    historyQueueStart =
                        (historyQueueStart + 1) % maxHistoryQueueLen;
                    currHistoryQueueLen--;
                }
                uint currIdx =
                    (historyQueueStart + currHistoryQueueLen) %
                        maxHistoryQueueLen;
                historyQueue[currIdx] = page.getBlockId();
                historyBitmap.set(
                    opaqueToInt(page.getBlockId()) % historyBitmap.size());
                currHistoryQueueLen++;
            }

            freshmenQueue.remove(page);
            TwoQDirtyPage &dirtyPageNode = page.TwoQVictim::getDirtyPageNode();
            if (dirtyPageNode.getDirtyState() ==
                TwoQDirtyPage::PAGE_DIRTY_FRESHMAN)
            {
                dirtyFreshmenQueue.remove(dirtyPageNode);
                dirtyPageNode.setDirtyState(TwoQDirtyPage::PAGE_CLEAN);
            } else {
                assert(isPageClean(page));
            }
        }

        // If the page is being discarded, remove it from the history bitmap.
        // Just clear the blockId from the history bitmap, but leave the
        // blockId in the history queue itself.  The page will get cleaned out
        // if we need to resize the history queue.
        if (discard) {
            historyBitmap.set(
                opaqueToInt(page.getBlockId()) % historyBitmap.size(),
                false);
        }

        page.TwoQVictim::setState(TwoQVictim::PAGE_STATE_FREE);
        assert(page.getNext() == NULL && page.getPrev() == NULL);
    }

    /**
     * Receives notification from CacheImpl that a page no longer needs to be
     * pinned.
     *
     * @param page the unpinned page
     */
    void notifyPageUnpin(PageT &page)
    {
        ExclusiveGuard exclusiveGuard(mutex);
        TwoQVictim::PageState state = page.TwoQVictim::getState();
        assert(
            state != TwoQVictim::PAGE_STATE_POPULAR_UNPINNED &&
            state != TwoQVictim::PAGE_STATE_FREE);

        // If the page is poular and pinned, move it to the popular-unpinned
        // queues.
        if (state == TwoQVictim::PAGE_STATE_POPULAR_PINNED) {
            popularPinnedQueue.remove(page);
            popularUnpinnedQueue.insertAtTail(page);
            TwoQDirtyPage &dirtyPageNode = page.TwoQVictim::getDirtyPageNode();
            if (dirtyPageNode.getDirtyState() ==
                TwoQDirtyPage::PAGE_DIRTY_POPULAR_PINNED)
            {
                dirtyPopularUnpinnedQueue.insertAtTail(dirtyPageNode);
                dirtyPageNode.setDirtyState(
                    TwoQDirtyPage::PAGE_DIRTY_POPULAR_UNPINNED);
            } else {
                assert(isPageClean(page));
            }
            page.TwoQVictim::setState(TwoQVictim::PAGE_STATE_POPULAR_UNPINNED);
        }
        // If the page state is free or a freshman, then there's nothing to do
    }

    /**
     * Receives notification from CacheImpl that a page has been marked as
     * dirty.
     *
     * @param page the dirty page
     */
    void notifyPageDirty(PageT &page)
    {
        ExclusiveGuard exclusiveGuard(mutex);
        TwoQVictim::PageState state = page.TwoQVictim::getState();
        assert(state != TwoQVictim::PAGE_STATE_FREE);

        TwoQDirtyPage &dirtyPageNode = page.TwoQVictim::getDirtyPageNode();
        assert(
            dirtyPageNode.getDirtyState() == TwoQDirtyPage::PAGE_CLEAN);

        // Set the dirty state to match the queue containing the page, and
        // then if appropriate, add the dirty node to the corresponding
        // dirty queue.  In the case of pages that are in the popular-pinned
        // queue, just set the dirty state since we don't have a dirty queue
        // for popular-pinned pages.  But we need to set the dirty state so
        // we know that the page is dirty when it's unpinned and moved to
        // the popular-unpinned queue.
        if (state == TwoQVictim::PAGE_STATE_POPULAR_UNPINNED) {
            dirtyPopularUnpinnedQueue.insertAtTail(dirtyPageNode);
            dirtyPageNode.setDirtyState(
                TwoQDirtyPage::PAGE_DIRTY_POPULAR_UNPINNED);
        } else if (state == TwoQVictim::PAGE_STATE_FRESHMAN) {
            dirtyFreshmenQueue.insertAtTail(dirtyPageNode);
            dirtyPageNode.setDirtyState(
                    TwoQDirtyPage::PAGE_DIRTY_FRESHMAN);
        } else if (state == TwoQVictim::PAGE_STATE_POPULAR_PINNED) {
            dirtyPageNode.setDirtyState(
                TwoQDirtyPage::PAGE_DIRTY_POPULAR_PINNED);
        }
    }

    /**
     * Receives notification from CacheImpl that a page is no longer dirty.
     *
     * @param page the clean page
     */
    void notifyPageClean(PageT &page)
    {
        ExclusiveGuard exclusiveGuard(mutex);
        TwoQVictim::PageState state = page.TwoQVictim::getState();
        // Note that it is possible for the page to be in the popular-pinned
        // queue if the page has just been pinned and the thread accessing
        // the page is waiting for I/O on the page to complete before using it.
        assert(state != TwoQVictim::PAGE_STATE_FREE);

        // Remove the dirty node from the queue if it's currently in a
        // queue, and then reset its dirty state to indicate that the page
        // is now clean.
        TwoQDirtyPage &dirtyPageNode = page.TwoQVictim::getDirtyPageNode();
        if (state == TwoQVictim::PAGE_STATE_POPULAR_UNPINNED) {
            assert(
                dirtyPageNode.getDirtyState() ==
                TwoQDirtyPage::PAGE_DIRTY_POPULAR_UNPINNED);
            dirtyPopularUnpinnedQueue.remove(dirtyPageNode);
        } else if (state == TwoQVictim::PAGE_STATE_FRESHMAN) {
            assert(
                dirtyPageNode.getDirtyState() ==
                TwoQDirtyPage::PAGE_DIRTY_FRESHMAN);
            dirtyFreshmenQueue.remove(dirtyPageNode);
        } else if (state == TwoQVictim::PAGE_STATE_POPULAR_PINNED) {
            assert(
                dirtyPageNode.getDirtyState() ==
                TwoQDirtyPage::PAGE_DIRTY_POPULAR_PINNED);
        }
        dirtyPageNode.setDirtyState(TwoQDirtyPage::PAGE_CLEAN);
    }

    /**
     * Receives notification from CacheImpl that a page has been discarded
     * from the cache.  This allows TwoQVictimPolicy to remove the page
     * from its history queue.
     *
     * @param blockId the blockId of the page being deallocated
     */
    void notifyPageDiscard(BlockId blockId)
    {
        ExclusiveGuard exclusiveGuard(mutex);
        // Just clear the blockId from the history bitmap, but leave the
        // blockId in the history queue itself.  The page will get cleaned out
        // if we need to resize the history queue.
        historyBitmap.set(opaqueToInt(blockId % historyBitmap.size()), false);
    }

    /**
     * Provides an SXMutex to CacheImpl to be acquired before
     * calling getVictimRange().  The mutex guard is held for the duration of
     * the iteration.
     *
     * @return a reference to the RW_Mutex protecting the queues
     */
    SXMutex &getMutex()
    {
        return mutex;
    }

    /**
     * Provides a range of candidate victims to CacheImpl.
     *
     * @return a pair of PageIterators, where pair.first references
     * the best victim and pair.second is the end of the victim range
     */
    std::pair<PageIterator,PageIterator> getVictimRange()
    {
        // If the freshmen queue has hit its size limit, victimize from
        // that queue first.  If all of its pages are pinned, then try
        // victimizing from the popular-unpinned page queue.  Do the
        // reverse if the freshmen queue has hit its size limit.

        if (freshmenQueue.size() < maxFreshmenQueueLen) {
            return std::pair<PageIterator, PageIterator>(
                PageIterator(
                    static_cast<PageT *>(popularUnpinnedQueue.getHead()),
                    static_cast<PageT *>(freshmenQueue.getHead())),
                PageIterator());
        } else {
            return std::pair<PageIterator, PageIterator>(
                PageIterator(
                    static_cast<PageT *>(freshmenQueue.getHead()),
                    static_cast<PageT *>(popularUnpinnedQueue.getHead())),
                PageIterator());
        }
    }

    /**
     * Provides a range of candidate victims for flushing to CacheImpl.
     *
     * @return a pair of DirtyPageIterators, where pair.first references
     * the best victim and pair.second is the end of the victim range
     */
    std::pair<DirtyPageIterator,DirtyPageIterator> getDirtyVictimRange()
    {
        // If the freshmen queue has hit its size limit, victimize from
        // that queue first.  If all of its pages are pinned, then try
        // victimizing from the popular-unpinned page queue.  Do the
        // reverse if the freshmen queue has hit its size limit.

        if (freshmenQueue.size() < maxFreshmenQueueLen) {
            return std::pair<DirtyPageIterator, DirtyPageIterator>(
                DirtyPageIterator(
                    static_cast<TwoQDirtyPage *>(
                        dirtyPopularUnpinnedQueue.getHead()),
                    static_cast<TwoQDirtyPage *>(dirtyFreshmenQueue.getHead())),
                DirtyPageIterator());
        } else {
            return std::pair<DirtyPageIterator, DirtyPageIterator>(
                DirtyPageIterator(
                    static_cast<TwoQDirtyPage *>(dirtyFreshmenQueue.getHead()),
                    static_cast<TwoQDirtyPage *>(
                        dirtyPopularUnpinnedQueue.getHead())),
                DirtyPageIterator());
        }
    }
};

FENNEL_END_NAMESPACE

#endif

// End TwoQVictimPolicy.h
