/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Technologies, Inc.
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

#ifndef Fennel_ExtMath_Included
#define Fennel_ExtMath_Included

#include "fennel/calc/RegisterReference.h"
#include "fennel/calc/ExtendedInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! Strcat. Ascii. dest = dest || str.
//!
//! Sets cbData to length for char as well as varchar. 
//!
//! If calling with fixed: Must call StrCatA3() first to have
//! length set correctly. Only then subsequent calls to strCatA2() are
//! possible.  If concatenating multiple strings, strCatA2 will honor
//! the intermediate length. 
//! After final call to strCatA2(), length should equal
//! width, to maintain fixed width string length == width. 
//! Behavior may be undefined if, after Calculator exits, length != width.
void
calcLn(RegisterRef<double> *result,
       RegisterRef<double> *x);

void
calcLog10(RegisterRef<double> *result,
	  RegisterRef<double> *x);

void
calcAbs(RegisterRef<double>* result,
	RegisterRef<double>* x);

void
calcAbs(RegisterRef<long long>* result,
	RegisterRef<long long>* x);

void
calcPow(RegisterRef<double>* result,
	RegisterRef<double>* x,
	RegisterRef<double>* y);

void
calcMod(RegisterRef<long long>* result,
	RegisterRef<long long>* x,
	RegisterRef<long long>* y);


class ExtendedInstructionTable;
        
void
ExtMathRegister(ExtendedInstructionTable* eit);


FENNEL_END_NAMESPACE

#endif

// End ExtMath.h
