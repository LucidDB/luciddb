/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
     * Receives notification from CacheImpl when a page is allocated,
     * giving the VictimPolicy a chance to initialize its own data structures
     * for this page.
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
     * Receives notification from CacheImpl when a page is freed, giving the
     * VictimPolicy a chance to deinitialize its own data structures for this
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
     * Receives notification from CacheImpl when a page is accessed.  On entry,
     * the page's mutex is held by the calling thread, so the state of the page
     * (e.g. its mapped BlockId) is guaranteed to remain stable.  This is true
     * for all other notify methods as well.  For LRUVictimPolicy, a page
     * access results in the page being moved to the MRU end of the list.
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
     * Receives notification from CacheImpl on a hint that a page
     * is a good candidate for victimization.  For LRUVictimPolicy, this
     * results in the page being moved to the LRU end of the list.
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
     * Receives notification from CacheImpl just after a page is mapped.  This
     * implies an access as well, so for efficiency no corresponding
     * notifyPageAccess notification is received.
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
     * Receives notification from CacheImpl just before a page is unmapped.
     * The Page object still has the ID being unmapped.
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
     * Provides an SXMutex to CacheImpl to be acquired before
     * calling getVictimRange().  The mutex guard is held for the duration of
     * the iteration.
     *
     * @return a reference to the RW_Mutex protecting the LRU list
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
        return std::pair<PageIterator,PageIterator>(
            PageIterator(pageLRU),PageIterator());
    }
};

FENNEL_END_NAMESPACE

#endif

// End LRUVictimPolicy.h
