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

#ifndef Fennel_OpaqueInteger_Included
#define Fennel_OpaqueInteger_Included

#include <iostream>

FENNEL_BEGIN_NAMESPACE

/**
 * OpaqueInteger is a wrapper for a primitive integer type.  It is not meant to
 * be used directly; instead, use the macros at the end of this file. If an
 * operation you need isn't here all ready, please add it.  However, do NOT add
 * an automatic cast operator or implicit constructor.  The point of this class
 * is to help prevent implicit conversions between values with different
 * semantics.  For example:
 *<p>
 *<pre><code>
 *
 *    DEFINE_OPAQUE_INTEGER(Apple,uint);
 *    DEFINE_OPAQUE_INTEGER(Orange,uint);
 *
 *    Apple apple_jack,apple_seed(20);
 *    Orange oj;
 *
 *    apple_jack = apple_seed+2; // legal
 *    oj = apple_jack;            // won't compile, can't compare apples&oranges
 *
 *</code></pre>
 *
 * Note that in a non-debug build, the macro expands to a simple typedef,
 * so there is no performance penalty for using it.  However, this also
 * means that you have to be careful with overloading to avoid release-build
 * breakage.
 *
 *<p>
 *
 * // TODO:  extend the mechanism to be able to handle
 * inheritance (e.g. BTreeId isa PageOwnerId).
 */
template<class T, class Dummy>
class OpaqueInteger
{
protected:
    T val;
public:
    OpaqueInteger()
    {
        // NOTE:  the default constructor should NOT
        // initialize to 0, because this doesn't match the non-DEBUG
        // behavior, AND actually breaks some  code (scary but true)
    }

    explicit OpaqueInteger(T t)
    {
        val = t;
    }

    int operator == (OpaqueInteger<T, Dummy> other) const
    {
        return val == other.val;
    }

    int operator != (OpaqueInteger<T, Dummy> other) const
    {
        return val != other.val;
    }

    int operator < (OpaqueInteger<T, Dummy> other) const
    {
        return val < other.val;
    }

    int operator > (OpaqueInteger<T, Dummy> other) const
    {
        return val > other.val;
    }

    int operator <= (OpaqueInteger<T, Dummy> other) const
    {
        return val <= other.val;
    }

    int operator >= (OpaqueInteger<T, Dummy> other) const
    {
        return val >= other.val;
    }

    OpaqueInteger<T, Dummy> operator ++ ()
    {
        return OpaqueInteger<T, Dummy>(++val);
    }

    OpaqueInteger<T, Dummy> operator ++ (int)
    {
        return OpaqueInteger<T, Dummy>(val++);
    }

    OpaqueInteger<T, Dummy> operator -- ()
    {
        return OpaqueInteger<T, Dummy>(--val);
    }

    OpaqueInteger<T, Dummy> operator -- (int)
    {
        return OpaqueInteger<T, Dummy>(val--);
    }

    OpaqueInteger<T, Dummy> operator *= (OpaqueInteger<T, Dummy> i)
    {
        val *= i.val;
        return *this;
    }

    OpaqueInteger<T, Dummy> operator /= (OpaqueInteger<T, Dummy> i)
    {
        val /= i.val;
        return *this;
    }

    OpaqueInteger<T, Dummy> operator += (OpaqueInteger<T, Dummy> i)
    {
        val += i.val;
        return *this;
    }

    OpaqueInteger<T, Dummy> operator -= (OpaqueInteger<T, Dummy> i)
    {
        val -= i.val;
        return *this;
    }

    OpaqueInteger<T, Dummy> operator %= (OpaqueInteger<T, Dummy> i)
    {
        val %= i.val;
        return *this;
    }

    OpaqueInteger<T, Dummy> operator *= (T t)
    {
        val *= t;
        return *this;
    }

    OpaqueInteger<T, Dummy> operator /= (T t)
    {
        val /= t;
        return *this;
    }

    OpaqueInteger<T, Dummy> operator += (T t)
    {
        val += t;
        return *this;
    }

    OpaqueInteger<T, Dummy> operator -= (T t)
    {
        val -= t;
        return *this;
    }

    OpaqueInteger<T, Dummy> operator %= (T t)
    {
        val %= t;
        return *this;
    }

    OpaqueInteger<T, Dummy> operator >>= (int i)
    {
        val >>= i;
        return *this;
    }

    OpaqueInteger<T, Dummy> operator <<= (int i)
    {
        val >>= i;
        return *this;
    }

    OpaqueInteger<T, Dummy> operator >> (int i) const
    {
        return OpaqueInteger<T, Dummy>(val >> i);
    }

    OpaqueInteger<T, Dummy> operator << (int i) const
    {
        return OpaqueInteger<T, Dummy>(val << i);
    }

    OpaqueInteger<T, Dummy> operator % (T i) const
    {
        return OpaqueInteger<T, Dummy>(val % i);
    }

    OpaqueInteger<T, Dummy> operator / (T i) const
    {
        return OpaqueInteger<T, Dummy>(val / i);
    }

    OpaqueInteger<T, Dummy> operator * (T i) const
    {
        return OpaqueInteger<T, Dummy>(val * i);
    }

    OpaqueInteger<T, Dummy> operator + (T t) const
    {
        return OpaqueInteger<T, Dummy>(val + t);
    }

    OpaqueInteger<T, Dummy> operator - (T t) const
    {
        return OpaqueInteger<T, Dummy>(val - t);
    }

    OpaqueInteger<T, Dummy> operator % (OpaqueInteger<T, Dummy> i) const
    {
        return OpaqueInteger<T, Dummy>(val % i.val);
    }

    OpaqueInteger<T, Dummy> operator / (OpaqueInteger<T, Dummy> i) const
    {
        return OpaqueInteger<T, Dummy>(val / i.val);
    }

    OpaqueInteger<T, Dummy> operator * (OpaqueInteger<T, Dummy> i) const
    {
        return OpaqueInteger<T, Dummy>(val * i.val);
    }

    OpaqueInteger<T, Dummy> operator + (OpaqueInteger<T, Dummy> i) const
    {
        return OpaqueInteger<T, Dummy>(val + i.val);
    }

    OpaqueInteger<T, Dummy> operator - (OpaqueInteger<T, Dummy> i) const
    {
        return OpaqueInteger<T, Dummy>(val - i.val);
    }

    T getWrapped() const
    {
        return val;
    }
};

/**
 * Use opaqueToInt to explicitly cast an OpaqueInteger back to the
 * wrapped type.
 */
template<class T, class Dummy>
inline T opaqueToInt(OpaqueInteger<T, Dummy> t)
{
    return t.getWrapped();
}

/**
 * Alternate definition of opaqueToInt to match the release build definition.
 */
template<class T>
inline T opaqueToInt(T t)
{
    return t;
}

#ifdef DEBUG
#define OPAQUE_INTEGER_AS_CLASS
#else
#define OPAQUE_INTEGER_AS_PRIMITIVE
#endif

#ifdef OPAQUE_INTEGER_AS_CLASS
// For debug build, define a dummy class to differentiate various
// OpaqueInteger classes of the same size.
#define DEFINE_OPAQUE_INTEGER(TypeName, TypeSize) \
class FENNEL_COMMON_EXPORT Dummy##TypeName { \
}; \
typedef OpaqueInteger<TypeSize, Dummy##TypeName> TypeName; \
typedef TypeSize TypeName##Primitive;

template<class T, class Dummy>
inline std::ostream &operator <<(std::ostream &o,OpaqueInteger<T, Dummy> t)
{
    o << opaqueToInt(t);
    return o;
}

#else

// For release build, just typedef because we don't trust the compiler.
#define DEFINE_OPAQUE_INTEGER(TypeName, TypeSize) \
typedef TypeSize TypeName; \
typedef TypeSize TypeName##Primitive;

#endif

// To refer to the primitive corresponding to a given opaque type T
// regardless of the build, use TPrimitive; e.g. if you
// DEFINE_OPAQUE_INTEGER(Cookie,uint64_t), then CookiePrimitive is
// always equivalent to uint64_t.

FENNEL_END_NAMESPACE

#endif

// End OpaqueInteger.h
