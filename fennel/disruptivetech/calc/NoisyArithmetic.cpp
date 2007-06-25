/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
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
//
// ---
//
// Arithmetic function on various types but raises exceptions on various
// error conditions (overflow, divide-by-zero etc.)
//
// TODO:
//      1. Need to research whether this usage of fenv is thread and
//          reentrant safe.
//      2. How does little/big endian effect the bit shift used in 
//          unsigned multiplication?
//      3. [placeholder] - why is op2 (second operand) passed to describe
//          helper @line 332 in NativeNativeInstruction.h? (this is a unary
//          operator & therefore has no 2nd operand.
//      4. Round operator?
//
*/
#include "fennel/common/CommonPreamble.h"

#include <assert.h>
#include <fenv.h>
#include <string>

#include "NoisyArithmetic.h"

FENNEL_BEGIN_CPPFILE("$Id$");

//#define NOISY_DISABLED    (1)

/* TODO --- check these codes: 220 DATA_EXCEPTION */
#define S_OVER    "22003"        /* NUMERIC_VALUE_OUT_OF_RANGE */
#define S_UNDR    "22000"        /* ?? underflow */
#define S_INVL    "22023"        /* INVALID_PARAMETER_VALUE */
#define S_INEX    "22000"        /* ?? inexact */
#define S_DIV0    "22012"        /* DIVISION_BY_ZERO */

#if defined(NOISY_DISABLED) && NOISY_DISABLED
/*
** Disabled all tests (to be compatible with current implementation
** in order to allow current tests to succeed
*/
#define DO(type)                                                            \
    template <> type Noisy<type>::add( TProgramCounter,                     \
        const type left, const type right ) throw( CalcMessage )            \
    { return left+right; }                                                  \
    template <> type Noisy<type>::sub( TProgramCounter,                     \
        const type left, const type right ) throw( CalcMessage )            \
    { return left-right; }                                                  \
    template <> type Noisy<type>::mul( TProgramCounter,                     \
        const type left, const type right ) throw( CalcMessage )            \
    { return left*right; }                                                  \
    template <> type Noisy<type>::div( TProgramCounter pc,                  \
        const type left, const type right ) throw( CalcMessage ) {          \
    if ( right==0 ) throw CalcMessage( S_DIV0, pc );                        \
    return left/right;                                                      \
    }                                                                       \
    template <> type Noisy<type>::neg( TProgramCounter,                     \
        const type right ) throw( CalcMessage )                             \
    { return right * -1; }                                      
DO(char)
DO(signed char)
DO(unsigned char)
DO(short)
DO(unsigned short)
DO(int)
DO(unsigned int)
DO(long long int)
DO(long long unsigned int)
DO(float)
DO(double)
DO(long double)

#else    /* not disabled */
#define UNSIGNED_ADD(type)                                                  \
                                                                            \
    template <> type Noisy<type>::add( TProgramCounter pc,                  \
            const type left, const type right )                             \
            throw( CalcMessage )                                            \
    {                                                                       \
    register type result;                                                   \
    if ( left > (std::numeric_limits<type>::max() - right) )                \
        throw CalcMessage( S_OVER, pc );                                    \
    result = left+right;                                                    \
    assert( result == (left+right) );                                       \
    return result;                                                          \
    }

#define SIGNED_ADD(type)                                                    \
    template <> type Noisy<type>::add( TProgramCounter pc,                  \
            const type left, const type right )                             \
            throw( CalcMessage )                                            \
    {                                                                       \
    register type result = left+right;                                      \
    if ( left<0 && right<0 && result>=0 )                                   \
        throw CalcMessage( S_OVER, pc );                                    \
    if ( left>0 && right>0 && result<=0 )                                   \
        throw CalcMessage( S_OVER, pc );                                    \
    assert( result == (left+right) );                                       \
    return result;                                                          \
    }

#define UNSIGNED_SUB(type)                                                  \
    template <> type Noisy<type>::sub( TProgramCounter pc,                  \
            const type left, const type right )                             \
            throw( CalcMessage )                                            \
    {                                                                       \
    register type result;                                                   \
    if ( right > left ) throw CalcMessage( S_OVER, pc );                    \
    return left-right;                                                      \
    result = left-right;                                                    \
    assert( result == (left-right) );                                       \
    return result;                                                          \
    }

#define SIGNED_SUB(type)                                                    \
    template <> type Noisy<type>::sub( TProgramCounter pc,                  \
            const type left, const type right )                             \
            throw( CalcMessage )                                            \
    {                                                                       \
    register type r = right;                                                \
    register type l = left;                                                 \
    register type result;                                                   \
    if ( r==std::numeric_limits<type>::min() ) {                            \
        if ( l==std::numeric_limits<type>::max() )                          \
            throw CalcMessage( S_OVER, pc );                                \
        r++;                                                                \
        l++;                                                                \
        }                                                                   \
    result = Noisy<type>::add( pc, l, -r );                                 \
    assert( result == (left-right) );                                       \
    return result;                                                          \
    }

#define UNSIGNED_MUL(type)                                                  \
    template <> type Noisy<type>::mul( TProgramCounter pc,                  \
            const type left, const type right )                             \
            throw( CalcMessage )                                            \
    {                                                                       \
    register type result;                                                   \
    if ( left==0 || right==0 ) return 0;                                    \
    if ( left>right ) return Noisy<type>::mul( pc, right, left );           \
    register type r = right;                                                \
    register type l = left;                                                 \
    assert( l<=r );                                                         \
    const type msb = ~( std::numeric_limits<type>::max() >> 1 );            \
    result = 0;                                                             \
    while(1) {                                                              \
        if ( l & 0x1 ) { result = Noisy<type>::add( pc, result, r ); }      \
        l >>= 1;                                                            \
        if ( !l ) break;                                                    \
        if ( msb & r ) throw CalcMessage( S_OVER, pc );                     \
        r <<= 1;                                                            \
        }                                                                   \
    assert( result == (left*right) );                                       \
    return result;                                                          \
    }

#define SIGNED_MUL(type)                                                    \
    template <> type Noisy<type>::mul( TProgramCounter pc,                  \
            const type left, const type right )                             \
            throw( CalcMessage )                                            \
    {                                                                       \
    register type result;                                                   \
    if ( left==0 || right==0 ) return 0;                                    \
    if ( right>left ) return Noisy<type>::mul( pc, right, left );           \
    register type r = right;                                                \
    register type l = left;                                                 \
    assert( r<=l );                                                         \
    if ( l<0 /* infers r<0, both negative */ ) {                            \
        if ( r==std::numeric_limits<type>::min() )                          \
            throw CalcMessage( S_OVER, pc );                                \
        assert( l!=std::numeric_limits<type>::min() );                      \
        l = -l;                                                             \
        r = -r;                                                             \
        }                                                                   \
    assert( l>0 );                                                          \
    const type n_max = std::numeric_limits<type>::min() >> 1;               \
    const type p_max = (-n_max) - 1;                                        \
    result = 0;                                                             \
    while(1) {                                                              \
        if ( l & 0x1 ) { result = Noisy<type>::add( pc, result, r ); }      \
        l >>= 1;                                                            \
        if ( !l ) break;                                                    \
        if ( r<n_max || r>p_max )                                           \
            throw CalcMessage( S_OVER, pc );                                \
        r *= 2;                                                             \
        }                                                                   \
    assert( result == (left*right) );                                       \
    return result;                                                          \
    }

#define UNSIGNED_DIV(type)                                                  \
    template <> type Noisy<type>::div( TProgramCounter pc,                  \
            const type left, const type right )                             \
            throw( CalcMessage )                                            \
    {                                                                       \
    if ( right==0 ) throw CalcMessage( S_DIV0, pc );                        \
    register type result = left / right;                                    \
    assert( result == (left/right) );                                       \
    return result;                                                          \
    }

#define SIGNED_DIV(type)                                                    \
    template <> type Noisy<type>::div( TProgramCounter pc,                  \
            const type left, const type right )                             \
            throw( CalcMessage )                                            \
    {                                                                       \
    /* this only holds for 2's complement representations */                \
    if ( left==std::numeric_limits<type>::min() && right==-1 )              \
        throw CalcMessage( S_OVER, pc );                                    \
                                                                            \
    if ( right==0 ) throw CalcMessage( S_DIV0, pc );                        \
    register type result = left / right;                                    \
    assert( result == (left/right) );                                       \
    return result;                                                          \
    }

#define UNSIGNED_NEG(type)                                                  \
    template <> type Noisy<type>::neg( TProgramCounter pc,                  \
            const type right ) throw( CalcMessage )                         \
    {                                                                       \
    if ( right!=0 ) throw CalcMessage( S_OVER, pc );                        \
    return 0;                                                               \
    }

#define SIGNED_NEG(type)                                                    \
    template <> type Noisy<type>::neg( TProgramCounter pc,                  \
            const type right ) throw( CalcMessage )                         \
    {                                                                       \
    /* this only holds for 2's complement representations */                \
    if ( right==std::numeric_limits<type>::min() )                          \
        throw CalcMessage( S_OVER, pc );                                    \
                                                                            \
    return -(right);                                                        \
    }

/* --- */
inline void maybe_raise_fe_exception( TProgramCounter pc )
        throw( CalcMessage )
{
    int fe = ::fetestexcept( FE_ALL_EXCEPT );
    if ( 0 ) ;
    else if ( fe & FE_DIVBYZERO ) {
        throw CalcMessage( S_DIV0, pc );
        }
    else if ( fe & FE_UNDERFLOW ) {
        throw CalcMessage( S_UNDR, pc );
        }
    else if ( fe & FE_OVERFLOW ) { 
        throw CalcMessage( S_OVER, pc );
        }
    else if ( fe & FE_INVALID ) {
        throw CalcMessage( S_INVL, pc );
        }

    /* leave this last because it occurs in conjunction with other
        flags */
    else if ( fe & FE_INEXACT ) {
        throw CalcMessage( S_INEX, pc );
        }
}

#define FLOATING_ADD(type)                                                  \
    template <> type Noisy<type>::add( TProgramCounter pc,                  \
            const type left, const type right )                             \
            throw( CalcMessage )                                            \
    {                                                                       \
    type result;                                                            \
    ::feclearexcept( FE_ALL_EXCEPT );                                       \
    result = left + right;                                                  \
    maybe_raise_fe_exception( pc );                                         \
    assert( result == (left+right) );                                       \
    return result;                                                          \
    }

#define FLOATING_SUB(type)                                                  \
    template <> type Noisy<type>::sub( TProgramCounter pc,                  \
            const type left, const type right )                             \
            throw( CalcMessage )                                            \
    {                                                                       \
    type result;                                                            \
    ::feclearexcept( FE_ALL_EXCEPT );                                       \
    result = left - right;                                                  \
    maybe_raise_fe_exception( pc );                                         \
    assert( result == (left-right) );                                       \
    return result;                                                          \
    }

#define FLOATING_MUL(type)                                                  \
    template <> type Noisy<type>::mul( TProgramCounter pc,                  \
            const type left, const type right )                             \
            throw( CalcMessage )                                            \
    {                                                                       \
    type result;                                                            \
    ::feclearexcept( FE_ALL_EXCEPT );                                       \
    result = left * right;                                                  \
    maybe_raise_fe_exception( pc );                                         \
    assert( result == (left*right) );                                       \
    return result;                                                          \
    }

#define FLOATING_DIV(type)                                                  \
    template <> type Noisy<type>::div( TProgramCounter pc,                  \
            const type left, const type right )                             \
            throw( CalcMessage )                                            \
    {                                                                       \
    type result;                                                            \
    ::feclearexcept( FE_ALL_EXCEPT );                                       \
    result = left / right;                                                  \
    maybe_raise_fe_exception( pc );                                         \
    assert( result == (left/right) );                                       \
    return result;                                                          \
    }

#define FLOATING_NEG(type)                                                  \
    template <> type Noisy<type>::neg( TProgramCounter pc,                  \
            const type right ) throw( CalcMessage )                         \
    {                                                                       \
    type result;                                                            \
    ::feclearexcept( FE_ALL_EXCEPT );                                       \
    result = (-right);                                                      \
    maybe_raise_fe_exception( pc );                                         \
    assert( result == (-right) );                                           \
    return result;                                                          \
    }

SIGNED_ADD(char)
SIGNED_ADD(signed char)
SIGNED_ADD(short)
SIGNED_ADD(int)
SIGNED_ADD(long long int)

UNSIGNED_ADD(unsigned char)
UNSIGNED_ADD(unsigned short)
UNSIGNED_ADD(unsigned int)
UNSIGNED_ADD(unsigned long long int)

SIGNED_SUB(char)
SIGNED_SUB(signed char)
SIGNED_SUB(short)
SIGNED_SUB(int)
SIGNED_SUB(long long int)

UNSIGNED_SUB(unsigned char)
UNSIGNED_SUB(unsigned short)
UNSIGNED_SUB(unsigned int)
UNSIGNED_SUB(unsigned long long int)

SIGNED_MUL(char)
SIGNED_MUL(signed char)
SIGNED_MUL(short)
SIGNED_MUL(int)
SIGNED_MUL(long long int)

UNSIGNED_MUL(unsigned char)
UNSIGNED_MUL(unsigned short)
UNSIGNED_MUL(unsigned int)
UNSIGNED_MUL(unsigned long long int)

SIGNED_DIV(char)
SIGNED_DIV(signed char)
SIGNED_DIV(short)
SIGNED_DIV(int)
SIGNED_DIV(long long int)

SIGNED_NEG(char)
SIGNED_NEG(signed char)
SIGNED_NEG(short)
SIGNED_NEG(int)
SIGNED_NEG(long long int)

UNSIGNED_DIV(unsigned char)
UNSIGNED_DIV(unsigned short)
UNSIGNED_DIV(unsigned int)
UNSIGNED_DIV(unsigned long long int)

UNSIGNED_NEG(unsigned char)
UNSIGNED_NEG(unsigned short)
UNSIGNED_NEG(unsigned int)
UNSIGNED_NEG(unsigned long long int)

FLOATING_ADD(float)
FLOATING_ADD(double)
FLOATING_ADD(long double)

FLOATING_SUB(float)
FLOATING_SUB(double)
FLOATING_SUB(long double)

FLOATING_MUL(float)
FLOATING_MUL(double)
FLOATING_MUL(long double)

FLOATING_DIV(float)
FLOATING_DIV(double)
FLOATING_DIV(long double)

FLOATING_NEG(float)
FLOATING_NEG(double)
FLOATING_NEG(long double)

#undef UNSIGNED_ADD
#undef SIGNED_ADD
#undef FLOATING_ADD
#undef UNSIGNED_SUB
#undef SIGNED_SUB
#undef FLOATING_SUB
#undef UNSIGNED_MUL
#undef SIGNED_MUL
#undef FLOATING_MUL
#undef UNSIGNED_DIV
#undef SIGNED_DIV
#undef FLOATING_DIV
#undef UNSIGNED_NEG
#undef SIGNED_NEG
#undef FLOATING_NEG

#endif /* disabled or not */

FENNEL_END_CPPFILE("$Id$");

