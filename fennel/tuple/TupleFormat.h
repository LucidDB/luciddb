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

#ifndef Fennel_TupleFormat_Included
#define Fennel_TupleFormat_Included

FENNEL_BEGIN_NAMESPACE

/**
 * TupleFormat enumerates the ways in which a tuple can be marshalled.
 */
enum TupleFormat
{
    /**
     * Standard tuple format.
     */
    TUPLE_FORMAT_STANDARD,

    /**
     * Treat variable-width  attributes as fixed width (using maximum width).
     * Allows nulls.
     */
    TUPLE_FORMAT_ALL_FIXED,

    /**
     * Same as standard, except all integers are stored in network byte
     * order.
     */
    TUPLE_FORMAT_NETWORK
};

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
#define ARCH_ALIGN_MASK (ARCH_ALIGN_BYTES - 1)

/**
 * TupleAlign enumerates the ways in which a tuple can be byte aligned
 */
enum TupleAlign
{
    /**
     * 4-byte alignment (for 32-bit machines)
     */
    TUPLE_ALIGN4 = 4,

    /**
     * 8-byte alignment (for 32-bit machines)
     */
    TUPLE_ALIGN8 = 8,

    /**
     * the alignment of the native platform
     */
    TUPLE_ALIGN_NATIVE = ARCH_ALIGN_BYTES
};

/**
 * Align a size UP to the next alignment multiple.
 */
template<class T>
inline T alignRoundUp(T t, TupleAlign align)
{
  //    if (t & ARCH_ALIGN_MASK) {
  //    return alignRoundDown(t, align) + ARCH_ALIGN_BYTES;
    if (t & (align - 1)) {
        return alignRoundDown(t, align) + align;
    } else {
        return t;
    }
}

/**
 * Align a pointer UP.
 */
template<class T>
inline T *alignRoundPtrUp(T *t, TupleAlign align)
{
    return (T *) alignRoundUp(uint(t), align);
}

/**
 * Align a size DOWN to the next alignment multiple.
 */
template<class T>
inline T alignRoundDown(T t, TupleAlign align)
{
    //return t & ~ARCH_ALIGN_MASK;
    return t & ~(align - 1);
}

/**
 * Align a pointer DOWN (assuming <code>sizeof(uint) == sizeof(void *)</code>).
 */
template<class T>
inline T *alignRoundPtrDown(T *t, TupleAlign align)
{
    return (T *) alignRoundDown(uint(t), align);
}

FENNEL_END_NAMESPACE

#endif

// End TupleFormat.h
