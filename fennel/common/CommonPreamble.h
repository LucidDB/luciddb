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

#ifndef Fennel_CommonPreamble_Included
#define Fennel_CommonPreamble_Included

// Autoconf definitions
#include <config.h>

// NOTE: CommonPreamble.h should be included by all fennel *.cpp files before
// any other.

// Common standard headers

// NOTE: we include these first to make sure we get the desired limit
// definitions
#ifndef __MSVC__
#define __STDC_LIMIT_MACROS
#else
#define NOMINMAX
#pragma warning(disable : 4355)
#ifdef _WIN64
#define __WORDSIZE 64
#else
#define __WORDSIZE 32
#endif
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE 1
#ifdef __APPLE__
#define _DARWIN_C_SOURCE
#endif

// Request support for large file offsets (> 4G) without needing special
// versions of standard I/O calls.
#define _FILE_OFFSET_BITS 64

#ifndef __MSVC__
#include <inttypes.h>
#endif

#include <stddef.h>
#include <limits.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <string>
#include <string.h>
#include <time.h>
#include <new>
#include <cassert>
#include <vector>
#include <boost/thread/tss.hpp>

#ifdef __APPLE__
#include <strings.h>
#define FMT_INT64      "lld"
#define FMT_UINT64     "llu"
#define isnan __inline_isnan
#else
#ifndef __MSVC__
#if __WORDSIZE == 64
#define FMT_INT64      "ld"
#define FMT_UINT64     "lu"
#else
#define FMT_INT64      "lld"
#define FMT_UINT64     "llu"
#endif
#else
// MSVCRT.DLL printf treats ll as a 32-bit integer
// and uses the prefix I64 for 64-bit integers
#define FMT_INT64      "I64d"
#define FMT_UINT64     "I64u"
#define strtoll _strtoi64
#define strtoull _strtoui64
#define strncasecmp strnicmp
#define isnan _isnan
// TODO jvs 3-Mar-2009:  inline function instead
#define round(x) (((x) >= 0) ? floor((x) + 0.5) : ceil((x) - 0.5))
#define roundf round

#endif
#endif

typedef unsigned uint;

// DLL export symbols for each component

#ifdef __MSVC__

#if defined(FENNEL_COMMON_EXPORTS) || defined(FENNEL_SYNCH_EXPORTS)
#define FENNEL_COMMON_EXPORT __declspec(dllexport)
#define FENNEL_SYNCH_EXPORT __declspec(dllexport)
#else
#define FENNEL_COMMON_EXPORT __declspec(dllimport)
#define FENNEL_SYNCH_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_DEVICE_EXPORTS
#define FENNEL_DEVICE_EXPORT __declspec(dllexport)
#else
#define FENNEL_DEVICE_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_CACHE_EXPORTS
#define FENNEL_CACHE_EXPORT __declspec(dllexport)
#else
#define FENNEL_CACHE_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_SEGMENT_EXPORTS
#define FENNEL_SEGMENT_EXPORT __declspec(dllexport)
#else
#define FENNEL_SEGMENT_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_TXN_EXPORTS
#define FENNEL_TXN_EXPORT __declspec(dllexport)
#else
#define FENNEL_TXN_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_TUPLE_EXPORTS
#define FENNEL_TUPLE_EXPORT __declspec(dllexport)
#else
#define FENNEL_TUPLE_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_EXEC_EXPORTS
#define FENNEL_EXEC_EXPORT __declspec(dllexport)
#else
#define FENNEL_EXEC_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_BTREE_EXPORTS
#define FENNEL_BTREE_EXPORT __declspec(dllexport)
#else
#define FENNEL_BTREE_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_DB_EXPORTS
#define FENNEL_DB_EXPORT __declspec(dllexport)
#else
#define FENNEL_DB_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_FTRS_EXPORTS
#define FENNEL_FTRS_EXPORT __declspec(dllexport)
#else
#define FENNEL_FTRS_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_SORTER_EXPORTS
#define FENNEL_SORTER_EXPORT __declspec(dllexport)
#else
#define FENNEL_SORTER_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_FLATFILE_EXPORTS
#define FENNEL_FLATFILE_EXPORT __declspec(dllexport)
#else
#define FENNEL_FLATFILE_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_HASHEXE_EXPORTS
#define FENNEL_HASHEXE_EXPORT __declspec(dllexport)
#else
#define FENNEL_HASHEXE_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_CALCULATOR_EXPORTS
#define FENNEL_CALCULATOR_EXPORT __declspec(dllexport)
#else
#define FENNEL_CALCULATOR_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_FARRAGO_EXPORTS
#define FENNEL_FARRAGO_EXPORT __declspec(dllexport)
#else
#define FENNEL_FARRAGO_EXPORT __declspec(dllimport)
#endif

#ifdef FENNEL_TEST_EXPORTS
#define FENNEL_TEST_EXPORT __declspec(dllexport)
#else
#define FENNEL_TEST_EXPORT __declspec(dllimport)
#endif

#if defined(FENNEL_LCS_EXPORTS) || defined(FENNEL_LBM_EXPORTS)
#define FENNEL_LCS_EXPORT __declspec(dllexport)
#define FENNEL_LBM_EXPORT __declspec(dllexport)
#else
#define FENNEL_LCS_EXPORT __declspec(dllimport)
#define FENNEL_LBM_EXPORT __declspec(dllimport)

#endif

#else
#define FENNEL_COMMON_EXPORT
#define FENNEL_SYNCH_EXPORT
#define FENNEL_DEVICE_EXPORT
#define FENNEL_CACHE_EXPORT
#define FENNEL_SEGMENT_EXPORT
#define FENNEL_TXN_EXPORT
#define FENNEL_TUPLE_EXPORT
#define FENNEL_EXEC_EXPORT
#define FENNEL_BTREE_EXPORT
#define FENNEL_DB_EXPORT
#define FENNEL_FTRS_EXPORT
#define FENNEL_SORTER_EXPORT
#define FENNEL_FLATFILE_EXPORT
#define FENNEL_HASHEXE_EXPORT
#define FENNEL_CALCULATOR_EXPORT
#define FENNEL_FARRAGO_EXPORT
#define FENNEL_TEST_EXPORT
#define FENNEL_LCS_EXPORT
#define FENNEL_LBM_EXPORT
#endif

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

template<class T, class Dummy>
struct hash
< fennel::OpaqueInteger<T, Dummy> >
{
    size_t operator() (const fennel::OpaqueInteger<T, Dummy> &key) const
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

class FENNEL_COMMON_EXPORT VectorOfUint : public std::vector<uint>
{
};

/**
 * Delete an object and set the pointer associated with
 * it to NULL.
 */
template <class T>
inline void deleteAndNullify(T *&p)
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
inline void deleteAndNullifyArray(T *&p)
{
    if (p) {
        delete [] p;
        p = NULL;
    }
}


// Memory alignment

// calculate number of bytes needed to hold given number of bits
inline uint bytesForBits(uint cBits)
{
    return (cBits >> 3) + ((cBits & 7) ? 1 : 0);
}

// Misc types and utils

// prints out a hex dump of the given block of memory
// cb bytes are dumped with at most 16 bytes per line, with the offset
// of each line printed on the left (with an optional additional offset)
extern void FENNEL_COMMON_EXPORT hexDump(
    std::ostream &,void const *,uint cb,
    uint cbOffsetInitial = 0);

template <class Numeric>
inline Numeric sqr(Numeric n)
{
    return n*n;
}

// NOTE jvs 18-Mar-2005:  neither boost nor stlport exposes this
extern int64_t FENNEL_COMMON_EXPORT getCurrentThreadId();

extern std::logic_error FENNEL_COMMON_EXPORT constructAssertion(
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
#define ntohll(x) \
    (((uint64_t) ntohl((uint32_t) (x)) << 32)\
     | ntohl((uint32_t) (x >> 32)))

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
