/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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

#ifndef Fennel_ExtRegExp_Included
#define Fennel_ExtRegExp_Included

#include "fennel/calculator/RegisterReference.h"
#include "fennel/calculator/ExtendedInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! StrLike. Ascii. Reuses pattern.
//! Pass a zero length string into escape if not defined
//! Passing a null into escape will result in null, per SQL99.
void
strLikeEscapeA(RegisterRef<bool>* result,
               RegisterRef<char*>* matchValue,
               RegisterRef<char*>* pattern,
               RegisterRef<char*>* escape);

//! StrLike. Ascii. Reuses pattern.
//! ESCAPE clause not defined.
void
strLikeA(RegisterRef<bool>* result,
         RegisterRef<char*>* matchValue,
         RegisterRef<char*>* pattern);

//! StrSimilar. Ascii. Reuses pattern.
//! Pass a zero length string into escape if not defined
//! Passing a null into escape will result in null, per SQL99 & SQL2003
void
strSimilarEscapeA(RegisterRef<bool>* result,
                  RegisterRef<char*>* matchValue,
                  RegisterRef<char*>* pattern,
                  RegisterRef<char*>* escape);

//! StrSimilar. Ascii. Reuses pattern.
//! ESCAPE clause not defined.
void
strSimilarA(RegisterRef<bool>* result,
            RegisterRef<char*>* matchValue,
            RegisterRef<char*>* pattern);

class ExtendedInstructionTable;

void
ExtRegExpRegister(ExtendedInstructionTable* eit);


FENNEL_END_NAMESPACE

#endif

// End ExtRegExp.h
