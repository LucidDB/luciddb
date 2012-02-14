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

#include "fennel/common/CommonPreamble.h"
#include "fennel/cache/VMAllocator.h"
#include "fennel/common/SysCallExcn.h"

#include <stdlib.h>
#ifndef __APPLE__
#include <malloc.h>
#endif

#ifdef HAVE_MMAP
#include <sys/resource.h>
#include <sys/mman.h>
#ifndef NDEBUG
#ifndef __APPLE__
#define FENNEL_CACHE_GUARD
#endif
#endif
#endif

#ifdef __APPLE__
#define MAP_ANONYMOUS MAP_ANON
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

VMAllocator::VMAllocator(size_t cbAllocInit, size_t nLocked)
{
    cbAlloc = cbAllocInit;
    nAllocs = 0;
    if (nLocked) {
#ifdef RLIMIT_MEMLOCK
        struct rlimit rl;
        if (::getrlimit(RLIMIT_MEMLOCK, &rl)) {
            throw SysCallExcn("getrlimit failed");
        }
        if (rl.rlim_cur != RLIM_INFINITY) {
            // REVIEW:  what if other stuff in the same process needs
            // locked pages?
            rl.rlim_cur = nLocked*cbAlloc;
            rl.rlim_max = nLocked*cbAlloc;
            if (::setrlimit(RLIMIT_MEMLOCK, &rl)) {
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
    // This assertion indicates that not all allocated pages were deallocated.
    // In relation to LER-5976: in debug builds, each allocation is divided
    // into 3 sections (two guard pages surrounding the actual allocation).
    // The mprotect calls in allocate(int *) can fail in low memory situations
    // (the kernel may allocate private memory as it tracks a single mmap
    // region being split in three).  When this occurs, subsequent munmap calls
    // (even on different regions) in deallocate sometimes fail, leading to a
    // failure of this assertion.  This should not occur in release builds,
    // since no guard pages are allocated or protected. NOTE: On Ubuntu Edgy
    // Eft (2.6.17-12-generic SMP), the munmap calls in deallocate fails three
    // times, then inexplicably begin succeeding.  Retrying the failed calls
    // then succeeds.
    assert(!getBytesAllocated());
}

void *VMAllocator::allocate(int *pErrorCode)
{
#ifdef HAVE_MMAP

    uint cbActualAlloc = cbAlloc;

#ifdef FENNEL_CACHE_GUARD
    // For a debug build, allocate "fence" regions before and after each
    // allocated buffer.  This helps to catch stray pointers around buffer
    // boundaries.  The fence size is one OS memory page, since that's
    // the minimum unit of protection, and also guarantees 512-byte
    // alignment for O_DIRECT file access.
    cbActualAlloc += 2*getpagesize();
#endif

    void *v = ::mmap(
        NULL, cbActualAlloc,
        PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS,-1,0);
    if (v == MAP_FAILED) {
        if (pErrorCode != NULL) {
            *pErrorCode = SysCallExcn::getCurrentErrorCode();
        }
        return NULL;
    }

#ifdef FENNEL_CACHE_GUARD
    PBuffer p = static_cast<PBuffer>(v);
    memset(p, 0xFE, getpagesize());
    if (::mprotect(p, getpagesize(), PROT_NONE)) {
        if (pErrorCode != NULL) {
            *pErrorCode = SysCallExcn::getCurrentErrorCode();
        }
        ::munmap(v, cbAlloc);
        return NULL;
    }
    p += getpagesize();
    memset(p, 0xFF, cbAlloc);
    v = p;
    p += cbAlloc;
    memset(p, 0xFE, getpagesize());
    if (::mprotect(p, getpagesize(), PROT_NONE)) {
        if (pErrorCode != NULL) {
            *pErrorCode = SysCallExcn::getCurrentErrorCode();
        }
        ::munmap(v, cbAlloc);
        return NULL;
    }
#endif

    if (bLockPages) {
        if (::mlock(v, cbAlloc)) {
            if (pErrorCode != NULL) {
                *pErrorCode = SysCallExcn::getCurrentErrorCode();
            }
            ::munmap(v, cbAlloc);
            return NULL;
        }
    }
#else
    void *v = malloc(cbAlloc);
    if (v == NULL) {
        if (pErrorCode != NULL) {
            *pErrorCode = SysCallExcn::getCurrentErrorCode();
        }
        return NULL;
    }
#endif
    ++nAllocs;
    return v;
}

int VMAllocator::deallocate(void *p, int *pErrorCode)
{
#ifdef HAVE_MMAP
    if (bLockPages) {
        if (::munlock(p, cbAlloc)) {
            if (pErrorCode != NULL) {
                *pErrorCode = SysCallExcn::getCurrentErrorCode();
            }
            return -1;
        }
    }

    uint cbActualAlloc = cbAlloc;
#ifdef FENNEL_CACHE_GUARD
    PBuffer p2 = static_cast<PBuffer>(p);
    p2 -= getpagesize();
    p = p2;
    cbActualAlloc += 2*getpagesize();
#endif
    if (::munmap((caddr_t)p, cbActualAlloc)) {
        if (pErrorCode != NULL) {
            *pErrorCode = SysCallExcn::getCurrentErrorCode();
        }
        return -1;
    }
#else
    free(p);
#endif
    --nAllocs;

    return 0;
}

size_t VMAllocator::getBytesAllocated() const
{
    return nAllocs*cbAlloc;
}

int VMAllocator::setProtection(
    void *pMem, uint cb, bool readOnly, int *pErrorCode)
{
    // FIXME jvs 7-Feb-2006:  use autoconf to get HAVE_MPROTECT instead
#ifdef HAVE_MMAP
    int prot = PROT_READ;
    if (!readOnly) {
        prot |= PROT_WRITE;
    }
    if (::mprotect(pMem, cb, prot)) {
        if (pErrorCode != NULL) {
            *pErrorCode = SysCallExcn::getCurrentErrorCode();
        }
        return -1;
    }
#endif

    return 0;
}

FENNEL_END_CPPFILE("$Id$");

// End VMAllocator.cpp
