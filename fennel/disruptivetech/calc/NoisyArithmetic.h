/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 SQLstream, Inc.
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
*/
#ifndef NoisyArithmetic_Included
#define NoisyArithmetic_Included

#include <stdexcept>
#include "CalcMessage.h"

FENNEL_BEGIN_NAMESPACE

/* ---
Struct for notification that an exception is forthcoming, allows
the callee to take some exception specific action with its opaque
element pData (such as cast it to a RegisterReference and call
toNull()).

Assumptions: if this structure exists the fnCB must not
be NULL.
 --- */
struct TExceptionCBData
{
    void (* fnCB)(const char *, void *);
    void *pData;
    TExceptionCBData( void (* fnTheCB)(const char *, void *), void *pTheData )
    :   fnCB( fnTheCB ),
        pData( pTheData ) {
        }
};

/* --- */
template <typename TMPL>
    struct Noisy {
        static TMPL add( TProgramCounter pc, const TMPL left, const TMPL right,
            TExceptionCBData *pExData ) throw( CalcMessage );
        static TMPL sub( TProgramCounter pc, const TMPL left, const TMPL right,
            TExceptionCBData *pExData ) throw( CalcMessage );
        static TMPL mul( TProgramCounter pc, const TMPL left, const TMPL right,
            TExceptionCBData *pExData ) throw( CalcMessage );
        static TMPL div( TProgramCounter pc, const TMPL left, const TMPL right,
            TExceptionCBData *pExData ) throw( CalcMessage );
        static TMPL neg( TProgramCounter pc, const TMPL right,
            TExceptionCBData *pExData ) throw( CalcMessage );
    };

FENNEL_END_NAMESPACE

#endif

// End NoisyAritchmetic.h

