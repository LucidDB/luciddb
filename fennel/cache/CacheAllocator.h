/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
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
     * @return the allocated chunk
     */
    virtual void *allocate() = 0;

    /**
     * Deallocates a chunk of memory.
     *
     * @param pMem the allocated memory
     */
    virtual void deallocate(void *pMem) = 0;

    /**
     * @return number of bytes currently allocated
     */
    virtual size_t getBytesAllocated() const = 0;
};

FENNEL_END_NAMESPACE

#endif

// End CacheAllocator.h
