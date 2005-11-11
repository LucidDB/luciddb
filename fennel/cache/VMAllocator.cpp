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

#include "fennel/common/CommonPreamble.h"
#include "fennel/cache/VMAllocator.h"
#include "fennel/common/SysCallExcn.h"

#include <stdlib.h>
#include <malloc.h>

#ifdef HAVE_MMAP
#include <sys/resource.h>
#include <sys/mman.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

VMAllocator::VMAllocator(size_t cbAllocInit,size_t nLocked)
{
    cbAlloc = cbAllocInit;
    nAllocs = 0;
    if (nLocked) {
#ifdef RLIMIT_MEMLOCK
        struct rlimit rl;
        if (::getrlimit(RLIMIT_MEMLOCK,&rl)) {
            throw SysCallExcn("getrlimit failed");
        }
        if (rl.rlim_cur != RLIM_INFINITY) {
            // REVIEW:  what if other stuff in the same process needs
            // locked pages?
            rl.rlim_cur = nLocked*cbAlloc;
            rl.rlim_max = nLocked*cbAlloc;
            if (::setrlimit(RLIMIT_MEMLOCK,&rl)) {
                throw SysCallExcn("setrlimit failed");
            }
        }
#endif
        bLockPages = true;
    } else {
        bLockPages = false;
    }
}

VMAllocator::~VMAllocator()
{
    assert(!getBytesAllocated());
}

void *VMAllocator::allocate()
{
#ifdef HAVE_MMAP

    uint cbActualAlloc = cbAlloc;
#ifndef NDEBUG
    // For a debug build, allocate "fence" regions before and after each
    // allocated buffer.  This helps to catch stray pointers around buffer
    // boundaries.  The fence size is one OS memory page, since that's
    // the minimum unit of protection, and also guarantees 512-byte
    // alignment for O_DIRECT file access.
    cbActualAlloc += 2*getpagesize();
#endif
    
    void *v = ::mmap(
        NULL,cbActualAlloc,
        PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS,-1,0);
    if (v == MAP_FAILED) {
        throw SysCallExcn("mmap failed");
    }
    
#ifndef NDEBUG
    PBuffer p = static_cast<PBuffer>(v);
    memset(p, 0xFE, getpagesize());
    if (::mprotect(p, getpagesize(), PROT_NONE)) {
        SysCallExcn excn("mprotect on pre-fence failed");
        ::munmap(v,cbAlloc);
        throw excn;
    }
    p += getpagesize();
    memset(p, 0xFF, cbAlloc);
    v = p;
    p += cbAlloc;
    memset(p, 0xFE, getpagesize());
    if (::mprotect(p, getpagesize(), PROT_NONE)) {
        SysCallExcn excn("mprotect on post-fence failed");
        ::munmap(v,cbAlloc);
        throw excn;
    }
#endif
    
    if (bLockPages) {
        if (::mlock(v,cbAlloc)) {
            SysCallExcn excn("mlock failed");
            ::munmap(v,cbAlloc);
            throw excn;
        }
    }
#else
    void *v = malloc(cbAlloc);
    if (v == NULL) {
        throw SysCallExcn("mmap failed");
    }
#endif
    ++nAllocs;
    return v;
}

void VMAllocator::deallocate(void *p)
{
#ifdef HAVE_MMAP
    if (bLockPages) {
        if (::munlock(p,cbAlloc)) {
            throw SysCallExcn("munlock failed");
        }
    }
    
    uint cbActualAlloc = cbAlloc;
#ifndef NDEBUG
    PBuffer p2 = static_cast<PBuffer>(p);
    p2 -= getpagesize();
    p = p2;
    cbActualAlloc += 2*getpagesize();
#endif
    if (::munmap((caddr_t)p,cbActualAlloc)) {
        throw SysCallExcn("munmap failed");
    }
#else
    free(p);
#endif
    --nAllocs;
}

size_t VMAllocator::getBytesAllocated() const
{
    return nAllocs*cbAlloc;
}

FENNEL_END_CPPFILE("$Id$");

// End VMAllocator.cpp
