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

    std::subtractive_rng randomNumberGenerator;

    friend class PageIterator;

public:
    // for use by CacheImpl when iterating over candidate victims
    typedef NullMutexGuard SharedGuard;
    typedef NullMutexGuard ExclusiveGuard;
    // TODO:  write an STL modulo_iterator
    class PageIterator
    {
        RandomVictimPolicy policy;
        uint iPage;

        PageT *getCurrent() const
        {
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

    typedef PageIterator DirtyPageIterator;

    RandomVictimPolicy()
    {
    }

    RandomVictimPolicy(const CacheParams &params)
    {
    }

    void setAllocatedPageCount(uint nCachePages)
    {
    }

    void registerPage(PageT &page)
    {
        pages.push_back(&page);
    }

    void unregisterPage(PageT &)
    {
        // TODO: zfong 1/8/08 - Should remove the page from the pages vector.
        // Otherwise, unallocated pages will be returned by getVictimRange().
    }

    void notifyPageAccess(PageT &, bool)
    {
    }

    void notifyPageNice(PageT &)
    {
    }

    void notifyPageMap(PageT &, bool)
    {
    }

    void notifyPageUnmap(PageT &, bool)
    {
    }

    void notifyPageUnpin(PageT &page)
    {
    }

    void notifyPageDirty(PageT &page)
    {
    }

    void notifyPageClean(PageT &page)
    {
    }

    void notifyPageDiscard(BlockId blockId)
    {
    }

    NullMutex &getMutex()
    {
        return nullMutex;
    }

    std::pair<PageIterator, PageIterator> getVictimRange()
    {
        uint iPage = randomNumberGenerator(pages.size());
        uint iPageEnd = iPage ? iPage-1 : pages.size();
        return std::pair<PageIterator, PageIterator>(
            PageIterator(*this,iPage),
            PageIterator(*this,iPageEnd));
    }

    std::pair<DirtyPageIterator, DirtyPageIterator> getDirtyVictimRange()
    {
        return
            static_cast<std::pair<DirtyPageIterator, DirtyPageIterator> >(
                getVictimRange());
    }
};

FENNEL_END_NAMESPACE

#endif

// End RandomVictimPolicy.h
