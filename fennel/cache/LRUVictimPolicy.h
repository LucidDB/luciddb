/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#ifndef Fennel_LRUVictimPolicy_Included
#define Fennel_LRUVictimPolicy_Included

#include "fennel/common/IntrusiveDList.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LRUVictim is the attributes class which must be a base for any CachePage type
 * which will be cached using an LRUVictimPolicy.  
 */
class LRUVictim : public IntrusiveDListNode
{
    // no data; base IntrusiveDListNode is used to order LRUVictims in an
    // intrusive doubly-linked list.
};

/**
 * LRUVictimPolicy implements the least-recently-used policy for cache
 * victimization.  It is a model for the VictimPolicy concept.  The public
 * documentation on this class also serves as a generic specification of the
 * VictimPolicy concept.
 *
 *<p>
 *
 * Note that any realization for PageT must inherit both CachePage and
 * LRUVictim as bases.
 * 
 */
template <class PageT>
class LRUVictimPolicy
{
    /**
     * SXMutex protecting LRU page chain.
     */
    SXMutex mutex;
    
    /**
     * Most-recently accessed page; this is one end of a queue implemented
     * as a doubly-linked list.
     */
    PageT *pageLRU;
    
    /**
     * Least-recently accessed page; this is the other end of a
     * queue implemented as a doubly-linked list.
     */
    PageT *pageMRU;

    /**
     * Guard for write access to mutex.
     */
    typedef SXMutexExclusiveGuard ExclusiveGuard;
    
public:

    /**
     * All models for VictimPolicy must define a nested public type
     * PageIterator, which is used by CacheImpl to iterate over candidate
     * victims in optimal order.  This type must be a model for a standard
     * forward iterator.  For LRUVictimPolicy, this is accomplished by
     * iterating over the doubly-linked list of pages from LRU to MRU.
     */
    typedef IntrusiveDListIter<PageT> PageIterator;

    /**
     * All models for VictimPolicy must define a nested public type SharedGuard,
     * which is held by CacheImpl while iterating over candidate victims.
     * This guard must provide any synchronization required to protect
     * the iteration against concurrent modifications.  The guard will be
     * initialized with the result of getMutex().  For LRUVictimPolicy, the
     * guard is a read guard protecting the doubly-linked list, meaning
     * notifications are blocked during iteration since they have to
     * modify the chain.
     */
    typedef SXMutexSharedGuard SharedGuard;

    /**
     * All models for VictimPolicy must have a default constructor.
     */
    LRUVictimPolicy()
    {
        pageLRU = NULL;
        pageMRU = NULL;
    }

    /**
     * Called by CacheImpl when a page is allocated.  This gives the
     * VictimPolicy a chance to initialize its own data structures for this
     * page.
     *
     * @param page the page being allocated
     */
    void registerPage(PageT &page)
    {
        // TODO:  use a DList container to avoid this edge case
        if (!pageLRU) {
            pageLRU = &page;
        } else {
            page.IntrusiveDListNode::insertAfter(*pageMRU);
        }
        pageMRU = &page;
    }

    /**
     * Called by CacheImpl when a page is freed.  This gives the
     * VictimPolicy a chance to initialize its own data structures for this
     * page.
     *
     * @param page the page being freed
     */
    void unregisterPage(PageT &)
    {
        // NOTE:  for now we assume that CacheImpl only registers pages on
        // initialization and unregisters them on shutdown (no dynamic page
        // allocation).  So don't bother unlinking the page.
    }

    /**
     * Called by CacheImpl when a page is accessed.  On entry, the page's mutex
     * is held by the calling thread, so the state of the page (e.g. its mapped
     * BlockId) is guaranteed to remain stable.  This is true for all other
     * notify methods as well.  For LRUVictimPolicy, a page access results in
     * the page being moved to the MRU end of the list.
     *
     * @param page the page being accessed
     */
    void notifyPageAccess(PageT &page)
    {
        ExclusiveGuard exclusiveGuard(mutex);
        if (&page == pageMRU) {
            return;
        }
        if (&page == pageLRU) {
            pageLRU = (PageT *) (page.getNext());
        }
        page.IntrusiveDListNode::detach();
        page.IntrusiveDListNode::insertAfter(*pageMRU);
        pageMRU = &page;
    }

    /**
     * Called by CacheImpl when a hint is received that a page is a
     * good candidate for victimization.  For LRUVictimPolicy, this results in
     * the page being moved to the LRU end of the list.
     *
     * @param page the page to which the hint pertains
     */
    void notifyPageNice(PageT &page)
    {
        ExclusiveGuard exclusiveGuard(mutex);
        if (&page == pageLRU) {
            return;
        }
        if (&page == pageMRU) {
            pageMRU = (PageT *) (page.getPrev());
        }
        page.IntrusiveDListNode::detach();
        page.IntrusiveDListNode::insertBefore(*pageLRU);
        pageLRU = &page;
    }

    /**
     * Called by CacheImpl just after a page is mapped.  This implies an access
     * as well, so for efficiency no corresponding notifyPageAccess call is
     * made.
     *
     * @param page the page being mapped
     */
    void notifyPageMap(PageT &page)
    {
        // first access for a newly mapped page will not get a corresponding
        // call to notifyPageAccess, so do it now
        notifyPageAccess(page);
    }

    /**
     * Called by CacheImpl just before a page is unmapped.  The Page
     * object still has the ID being unmapped.
     *
     * @param page the page being unmapped
     */
    void notifyPageUnmap(PageT &page)
    {
        // move the unmapped page to the MRU position so that it will not be
        // treated as a candidate for flush
        notifyPageAccess(page);
    }

    /**
     * Called by CacheImpl to initialize a SharedGuard before calling
     * getVictimRange().  The mutex guard is held for the duration of
     * the iteration.
     *
     * @return a reference to the RW_Mutex protecting the LRU list
     */
    SXMutex &getMutex()
    {
        return mutex;
    }

    /**
     * Called by CacheImpl to obtain a range of candidate victims.
     *
     * @return a pair of PageIterators, where pair.first references
     * the best victim and pair.second is the end of the victim range
     */
    std::pair<PageIterator,PageIterator> getVictimRange()
    {
        return std::pair<PageIterator,PageIterator>(
            PageIterator(pageLRU),PageIterator());
    }
};

FENNEL_END_NAMESPACE

#endif

// End LRUVictimPolicy.h
