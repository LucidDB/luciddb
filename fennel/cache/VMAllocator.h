/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#ifndef Fennel_VMAllocator_Included
#define Fennel_VMAllocator_Included

#include "fennel/cache/CacheAllocator.h"

FENNEL_BEGIN_NAMESPACE

/**
 * VMAllocator is an implementation of the CacheAllocator interface in
 * terms of OS page allocation calls.
 */
class FENNEL_CACHE_EXPORT VMAllocator
    : public CacheAllocator
{
    size_t cbAlloc;
    size_t nAllocs;
    bool bLockPages;

public:
    /**
     * Constructs a new VMAllocator.
     *
     * @param cbAlloc number of bytes per allocation; all calls
     * to allocate must specify this same value
     *
     * @param nLocked if nonzero, specifies that allocations should be
     * locked in memory, and that no more than the specified number
     * will be allocated at any one time
     */
    explicit VMAllocator(size_t cbAlloc,size_t nLocked = 0);

    virtual ~VMAllocator();

// ----------------------------------------------------------------------
// Implementation of CacheAllocator interface
// ----------------------------------------------------------------------
    virtual void *allocate(int *pErrorCode = NULL);
    virtual int deallocate(void *pMem, int *pErrorCode = NULL);
    virtual int setProtection(
        void *pMem, uint cb, bool readOnly, int *pErrorCode = NULL);
    virtual size_t getBytesAllocated() const;
};

FENNEL_END_NAMESPACE

#endif

// End VMAllocator.h
