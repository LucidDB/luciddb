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

#ifndef Fennel_CacheStats_Included
#define Fennel_CacheStats_Included

FENNEL_BEGIN_NAMESPACE

/**
 * CacheStats defines performance/activity statistics collected by the cache;
 * these can be obtained as a snapshot from Cache::collectStats().
 */
class CacheStats 
{
public:
    /**
     * Number of times a page access was satisfied without a disk
     * read (since last snapshot).
     */
    uint nHits;
    
    /**
     * Number of times a page access was satisfied without a disk
     * read (since cache initialization).
     */
    uint nHitsSinceInit;

    /**
     * Number of page accesses (since last snapshot).
     */
    uint nRequests;

    /**
     * Number of page accesses (since cache initialization).
     */
    uint nRequestsSinceInit;

    /**
     * Number of times a page had to be discarded to satisfy a request for
     * another page (since last snapshot).
     */
    uint nVictimizations;

    /**
     * Number of times a page had to be discarded to satisfy a request for
     * another page (since cache initialization).
     */
    uint nVictimizationsSinceInit;

    /**
     * Number of dirty pages (instantaneous).
     */
    uint nDirtyPages;

    /**
     * Number of disk pages read (since last snapshot).
     */
    uint nPageReads;

    /**
     * Number of disk pages read (since cache initialization).
     */
    uint nPageReadsSinceInit;

    /**
     * Number of disk pages written (since last snapshot).
     */
    uint nPageWrites;

    /**
     * Number of disk pages written (since cache initialization).
     */
    uint nPageWritesSinceInit;

    /**
     * Number of memory pages currently allocated in buffer pool
     * (instantaneous).
     */
    uint nMemPagesAllocated;

    /**
     * Number of memory pages currently allocated but unused
     * (instantaneous).
     */
    uint nMemPagesUnused;

    /**
     * Maximum number of memory pages which can be allocated in buffer pool
     * (immutable after cache initialization).
     */
    uint nMemPagesMax;
};

FENNEL_END_NAMESPACE

#endif

// End CacheStats.h
