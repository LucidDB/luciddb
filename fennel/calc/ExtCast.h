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

#ifndef Fennel_ExtCast_Included
#define Fennel_ExtCast_Included

#include "fennel/calc/RegisterReference.h"
#include "fennel/calc/ExtendedInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! castA. Ascii. Char & Varchar
//! 
//! Cast an exact numeric to an Ascii string.
//!
//! May throw "22001" data exception - string data, right truncation
void
castExactToStrA(RegisterRef<char*>* result,
                RegisterRef<int64_t>* src);

//! castA. Ascii. Char & Varchar
//! 
//! Cast an approximate numeric to an Ascii string.
//!
//! May throw "22001" data exception - string data, right truncation
void
castApproxToStrA(RegisterRef<char*>* result,
                 RegisterRef<double>* src);

//! castA. Ascii. Char & Varchar
//! 
//! Cast a string to an exact numeric.
//!
//! May throw "22018" data exception - invalid character value for cast
void
castStrtoExactA(RegisterRef<int64_t>* result,
                RegisterRef<char*>* src);

//! castA. Ascii. Char & Varchar
//! 
//! Cast a string to an approximate numeric.
//!
//! May throw "22018" data exception - invalid character value for cast
void
castStrToApproxA(RegisterRef<double>* result,
                 RegisterRef<char*>* src);

//! castA. Ascii. String to Varchar
//!
//! Cast a string to a variable-length string.
//!
//! May throw "22001" string data, right truncation
void
castStrToVarCharA(RegisterRef<char*>* result,
                  RegisterRef<char*>* src);

//! castA. Ascii. String to Char
//!
//! Cast a string to a fixed-length string.
//!
//! May throw "22001" string data, right truncation
void
castStrToCharA(RegisterRef<char*>* result,
               RegisterRef<char*>* src);


class ExtendedInstructionTable;
        
void
ExtCastRegister(ExtendedInstructionTable* eit);


FENNEL_END_NAMESPACE

#endif

// End ExtCast.h
