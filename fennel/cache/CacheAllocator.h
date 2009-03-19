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
     * @param pErrorCode on error and if non-NULL, the int referenced is
     * modified to contain the OS error code
     *
     * @return the allocated chunk; NULL if memory cannot be allocated (see
     * pErrorCode for OS error code)
     */
    virtual void *allocate(int *pErrorCode = NULL) = 0;

    /**
     * Deallocates a chunk of memory.
     *
     * @param pMem the allocated memory
     *
     * @param pErrorCode on error and if non-NULL, the int referenced is
     * modified to contain the OS error code
     *
     * @return 0 on success; -1 if memory cannot be deallocated (see
     * pErrorCode for OS error code)
     */
    virtual int deallocate(void *pMem, int *pErrorCode = NULL) = 0;

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
     * @param pErrorCode on error and if non-NULL, the int referenced is
     * modified to contain the OS error code
     *
     * @return 0 on success; -1 if an error occurs while manupulating memory
     * protections (see pErrorCode for OS error code)
     */
    virtual int setProtection(
        void *pMem, uint cb, bool readOnly, int *pErrorCode = NULL) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End CacheAllocator.h
