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

#ifndef Fennel_RandomVictimPolicy_Included
#define Fennel_RandomVictimPolicy_Included

#include <functional>
#include "fennel/synch/NullMutex.h"

FENNEL_BEGIN_NAMESPACE

/**
 * RandomVictimPolicy implements the random policy for cache
 * victimization.  It is a model for the VictimPolicy concept.  See
 * LRUVictimPolicy for general information on VictimPolicy models.
 *
 *<p>
 *
 * RandomVictimPolicy is intended mostly as a straw-man for benchmarking
 * purposes.  However, its simplicity (no synchronization required) could make
 * it a reasonable choice for very large data sets accessed with little
 * locality of reference.  Note that it's really not very random at the
 * moment.  TODO:  improve randomness by occasionally permuting the pages
 * array; but this could require synchronization.
 */
template <class PageT>
class RandomVictimPolicy
{
    /**
     * Use a null mutex to dummy out all locking requested by the cache.
     */
    NullMutex nullMutex;

    /**
     * Array of registered pages.
     */
    std::vector<PageT *> pages;

    bool bClosed;

    std::subtractive_rng randomNumberGenerator;

    friend class PageIterator;

public:
    // for use by CacheImpl when iterating over candidate victims
    typedef NullMutexGuard SharedGuard;
    // TODO:  write an STL modulo_iterator
    class PageIterator
    {
        RandomVictimPolicy policy;
        uint iPage;

        PageT *getCurrent() const
        {
            assert(!policy.bClosed);
            return policy.pages[iPage];
        }
        
    public:
        PageIterator(RandomVictimPolicy &policyInit,uint iPageInit)
            : policy(policyInit)
        {
            iPage = iPageInit;
        }

        void operator ++ ()
        {
            iPage++;
            if (iPage >= policy.pages.size()) {
                iPage = 0;
            }
        }

        PageT *operator -> () const
        {
            return getCurrent();
        }
        
        operator PageT * () const
        {
            return getCurrent();
        }
        
        PageT &operator * () const
        {
            return *getCurrent();
        }

        bool operator == (PageIterator const &other) const
        {
            // NOTE:  assume policy object is same
            return iPage == other.iPage;
        }
    };
    
    RandomVictimPolicy()
    {
        bClosed = false;
    }

    // NOTE:  for now we assume that CacheImpl only registers pages
    // on initialization and unregisters them on shutdown (no dynamic page
    // allocation).
    
    void registerPage(PageT &page)
    {
        pages.push_back(&page);
    }

    void unregisterPage(PageT &)
    {
        bClosed = true;
    }

    void notifyPageAccess(PageT &)
    {
        assert(!bClosed);
    }

    void notifyPageNice(PageT &)
    {
        assert(!bClosed);
    }

    void notifyPageMap(PageT &)
    {
        assert(!bClosed);
    }

    void notifyPageUnmap(PageT &)
    {
        assert(!bClosed);
    }

    NullMutex &getMutex()
    {
        assert(!bClosed);
        return nullMutex;
    }

    std::pair<PageIterator,PageIterator> getVictimRange()
    {
        uint iPage = randomNumberGenerator(pages.size());
        uint iPageEnd = iPage ? iPage-1 : pages.size();
        return std::pair<PageIterator,PageIterator>(
            PageIterator(*this,iPage),
            PageIterator(*this,iPageEnd));
    }
};

FENNEL_END_NAMESPACE

#endif

// End RandomVictimPolicy.h
