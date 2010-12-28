/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

#ifndef Fennel_PageBucket_Included
#define Fennel_PageBucket_Included

#include "fennel/cache/CachePage.h"
#include "fennel/synch/SXMutex.h"

FENNEL_BEGIN_NAMESPACE

/**
 * PageBucket represents a bucket of Pages which share something in common.
 * For example, in CacheImpl, pages are hashed into buckets based on their
 * BlockIds; a collection of free pages is also maintained as a bucket.
 */
template <class PageT>
class PageBucket
{
public:

    // typedefs for the list of Pages in this bucket; CachePage objects have
    // embedded links dedicated for this purpose to avoid dynamic allocation for
    // each map/unmap operation.
    typedef IntrusiveList<PageT, PageBucketListNode> PageList;
    typedef IntrusiveListIter<PageT, PageBucketListNode> PageListIter;
    typedef IntrusiveListMutator<PageT, PageBucketListNode> PageListMutator;

    /**
     * SXMutex protecting this bucket.
     */
    SXMutex mutex;

    /**
     * List of pages in this bucket.
     */
    PageList pageList;

    explicit PageBucket()
    {
    }
};

FENNEL_END_NAMESPACE

#endif

// End PageBucket.h
