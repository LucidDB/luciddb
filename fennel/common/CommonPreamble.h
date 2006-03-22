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

#ifndef Fennel_CommonPreamble_Included
#define Fennel_CommonPreamble_Included

// Autoconf definitions
#include <config.h>

// NOTE: CommonPreamble.h should be included by all fennel *.cpp files before
// any other.

// Common standard heades

// NOTE: we include these first to make sure we get the desired limit
// definitions
#ifndef __MINGW32__
#define __STDC_LIMIT_MACROS
#define FMT_INT64      "lld"
#define FMT_UINT64     "llu"
#else
// Mingw uses MSVCRT.DLL for printf, which treats ll as a 32-bit integer
// and uses the prefix I64 for 64 integers
#define FMT_INT64      "I64d"
#define FMT_UINT64     "I64u"
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE 1

// Request support for large file offsets (> 4G) without needing special
// versions of standard I/O calls.
#define _FILE_OFFSET_BITS 64

#include <inttypes.h>
#include <stddef.h>
#include <limits.h>

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <string>
#include <string.h>
#include <time.h>
#include <new>
#include <cassert>
#include <boost/thread/tss.hpp>

// FIXME:  correct port
typedef unsigned uint;

#include "fennel/common/Namespace.h"

// These macros are used to bracket code in all C++ files making up the
// implementation of the record manager.  The parameter x should be a string
// containing a revision control file Id tag, which is currently unused.

#define FENNEL_BEGIN_CPPFILE(x) \
FENNEL_BEGIN_NAMESPACE

#define FENNEL_END_CPPFILE(x) \
FENNEL_END_NAMESPACE

#include "fennel/common/OpaqueInteger.h"
#include "fennel/common/Types.h"

// define OpaqueInteger hash fxn
namespace std
{

template<class T,class Dummy>
struct hash< fennel::OpaqueInteger<T,Dummy> >
{
    size_t operator() (const fennel::OpaqueInteger<T,Dummy> &key) const
    {
        return hash<T>()(fennel::opaqueToInt(key));
    }
};
 
} // namespace std

// Memory management

// "placement new" definitions
inline void *operator new(size_t,fennel::PBuffer pBuffer)
{
    return pBuffer;
}

inline void *operator new[](size_t,fennel::PBuffer pBuffer)
{
    return pBuffer;
}

FENNEL_BEGIN_NAMESPACE

/**
 * Delete an object and set the pointer associated with
 * it to NULL.
 */
template <class T>
inline void deleteAndNullify(T *p)
{
    if (p) {
        delete p;
        p = NULL;
    }
}

/**
 * Delete an array and set the pointer associated with
 * it to NULL.
 */
template <class T>
inline void deleteAndNullifyArray(T *p)
{
    if (p) {
        delete [] p;
        p = NULL;
    }
}


// Memory alignment

/**
 * \def ARCH_ALIGN_BYTES
 * Note that on SPARC, uint64_t access has to be 64-bit aligned even for
 * a 32-bit processor
 * // TODO:  make this a const rather than a define; would a
 *       uint work for all types T below?
 */
#if (__WORDSIZE == 64) || defined(sun)
#define ARCH_ALIGN_BYTES 8
#else
#define ARCH_ALIGN_BYTES 4
#endif

/**
 * A bitmask which selects the unaligned bits of a memory address or size.
 */
#define ARCH_ALIGN_MASK (ARCH_ALIGN_BYTES-1)

/**
 * Align a size DOWN to the next alignment multiple.
 */
template<class T>
inline T alignRoundDown(T t)
{
    return t & ~ARCH_ALIGN_MASK;
}

/**
 * Align a pointer DOWN (assuming <code>sizeof(uint) == sizeof(void *)</code>).
 */
template<class T>
inline T *alignRoundPtrDown(T *t)
{
    return (T *) alignRoundDown(uint(t));
}

/**
 * Align a size UP to the next alignment multiple.
 */
template<class T>
inline T alignRoundUp(T t)
{
    if (t & ARCH_ALIGN_MASK) {
        return alignRoundDown(t) + ARCH_ALIGN_BYTES;
    } else {
        return t;
    }
}

/**
 * Align a pointer UP.
 */
template<class T>
inline T *alignRoundPtrUp(T *t)
{
    return (T *) alignRoundUp(uint(t));
}

// calculate number of bytes needed to hold given number of bits
inline uint bytesForBits(uint cBits)
{
    return (cBits>>3) + ((cBits & 7) ? 1 : 0);
}


// Misc types and utils

// prints out a hex dump of the given block of memory
// cb bytes are dumped with at most 16 bytes per line, with the offset
// of each line printed on the left (with an optional additional offset)
extern void hexDump(
    std::ostream &,void const *,uint cb,
    uint cbOffsetInitial = 0);

template <class Numeric>
inline Numeric sqr(Numeric n)
{
    return n*n;
}

// NOTE jvs 18-Mar-2005:  neither boost nor stlport exposes this
extern int getCurrentThreadId();

extern std::logic_error constructAssertion(
    char const *pFilename,int lineNum,char const *condExpr);

// Use permAssert to create an assertion which should be compiled even in
// non-debug builds.  This is only appropriate for performance-insensitive
// code. In debug-mode, does the same as assert.

#ifdef NDEBUG

#define permAssert(cond) \
do { \
    if (!(cond)) { \
        throw fennel::constructAssertion(__FILE__,__LINE__,#cond); \
    } \
} while (0)

#else

#define permAssert(cond) assert(cond)

#endif

// Use permFail to create an exception which indicates that an invariant has
// been violated. For example:
//
// switch (x) {
// case 1: return foo();
// default: permFail("invalid value for x: " << x);
// }

#ifdef NDEBUG

#define permFail(msg) \
do { \
    std::ostringstream oss; \
    oss << msg; \
    throw fennel::constructAssertion(__FILE__,__LINE__, oss.str().c_str()); \
} while (0)

#else

#define permFail(msg) \
do { \
    std::cout << "Internal error: " << msg << std::endl; \
    assert(false); \
    throw 0; \
} while (0)

#endif

// REVIEW: JK 1/19/2006: Replace with cpu_to_be64(x) ?
// Network to Host conversion for 64 bit quantities
#define ntohll(x) ( ( (uint64_t) ntohl ((uint32_t)( x )) << 32 ) |  \
                    ntohl ((uint32_t)(x >> 32))) 

#define htonll(x) ntohll(x)

// Asynchronous I/O API selection

#if defined(HAVE_LIBAIO_H) && defined(HAVE_LIBAIO)
#define USE_LIBAIO_H
#elif HAVE_AIO_H
#define USE_AIO_H
#endif

FENNEL_END_NAMESPACE

#endif

// End CommonPreamble.h
