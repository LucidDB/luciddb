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
     * Number of page accesses (since last snapshot).
     */
    uint nRequests;

    /**
     * Number of times a page had to be discarded to satisfy a request for
     * another page (since last snapshot).
     */
    uint nVictimizations;

    /**
     * Number of dirty pages (instantaneous).
     */
    uint nDirtyPages;

    /**
     * Number of disk pages read (since last snapshot).
     */
    uint nPageReads;

    /**
     * Number of disk pages written (since last snapshot).
     */
    uint nPageWrites;
};

FENNEL_END_NAMESPACE

#endif

// End CacheStats.h
