/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
//
// Test Calculator object directly by instantiating instruction objects,
// creating programs, running them, and checking the register set values.
*/

#ifndef Fennel_ExtRegExp_Included
#define Fennel_ExtRegExp_Included

#include "fennel/calc/RegisterReference.h"
#include "fennel/calc/ExtendedInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! StrLike. Ascii. Reuses pattern.
//! Pass a zero length string into escape if not defined
//! Passing a null into escape will result in null, per SQL99.
void
strLikeA(RegisterRef<bool>* result,   
         RegisterRef<char*>* matchValue,
         RegisterRef<char*>* pattern,
         RegisterRef<char*>* escape);

//! StrSimilar. Ascii. Reuses pattern.
//! Pass a zero length string into escape if not defined
//! Passing a null into escape will result in null, per SQL99 & SQL2003
void
strSimilarA(RegisterRef<bool>* result,   
            RegisterRef<char*>* matchValue,
            RegisterRef<char*>* pattern,
            RegisterRef<char*>* escape);

class ExtendedInstructionTable;
        
void
ExtRegExpRegister(ExtendedInstructionTable* eit);


FENNEL_END_NAMESPACE

#endif

// End ExtRegExp.h
