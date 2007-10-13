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

#ifndef Fennel_CacheAllocator_Included
#define Fennel_CacheAllocator_Included

FENNEL_BEGIN_NAMESPACE

/**
 * CacheAllocator defines an interface for allocating memory pages to be used
 * by the cache.
 */
class CacheAllocator
{
public:
    virtual ~CacheAllocator();

    /**
     * Allocates a chunk of memory of size determined by the constructor.
     *
     * @return the allocated chunk; NULL if memory cannot be allocated
     */
    virtual void *allocate() = 0;

    /**
     * Deallocates a chunk of memory.
     *
     * @param pMem the allocated memory
     *
     * @return 0 on success; -1 if memory cannot be deallocated
     */
    virtual int deallocate(void *pMem) = 0;

    /**
     * @return number of bytes currently allocated
     */
    virtual size_t getBytesAllocated() const = 0;

    /**
     * Changes protection state for a contiguous range of virtual memory.
     *
     * @param pMem start of range
     *
     * @param cb number of bytes in range
     *
     * @param readOnly true for read-only; false for read-write
     * (TODO jvs 7-Feb-2006:  support no-access as well)
     *
     * @return 0 on success; -1 if memory cannot be deallocated
     */
    virtual int setProtection(void *pMem, uint cb, bool readOnly) = 0;

    /**
     * Retrieve the OS error code for the last failed method call on this
     * CacheAllocator.  The return value of this function is only valid
     * until the next call to another function in this class.  In particular, 
     * this value is invalid after calls that return success.
     *
     * @return OS error code for the last failure encountered
     */
    virtual int getLastErrorCode() const = 0;
};

FENNEL_END_NAMESPACE

#endif

// End CacheAllocator.h
