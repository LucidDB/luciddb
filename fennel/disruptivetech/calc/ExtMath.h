/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

#ifndef Fennel_ExtMath_Included
#define Fennel_ExtMath_Included

#include "fennel/disruptivetech/calc/RegisterReference.h"
#include "fennel/disruptivetech/calc/ExtendedInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! mathLn. Calculates the natural logarithm
void
mathLn(RegisterRef<double> *result,
       RegisterRef<double> *x);

void
mathLn(RegisterRef<double> *result,
       RegisterRef<long long> *x);

//! mathLog10. Calculates the base-ten logarithm
void
mathLog10(RegisterRef<double> *result,
	  RegisterRef<double> *x);

//! mathAbs. Returns the absolute value.
void
mathAbs(RegisterRef<double>* result,
	RegisterRef<double>* x);

//! mathAbs. Returns the absolute value.
void
mathAbs(RegisterRef<long long>* result,
	RegisterRef<long long>* x);

//! mathPow. Calculates x^y. 
//!
//! Throws an error and sets the result to null if x<0 and y is not an integer value
void
mathPow(RegisterRef<double>* result,
	RegisterRef<double>* x,
	RegisterRef<double>* y);


class ExtendedInstructionTable;
        
void
ExtMathRegister(ExtendedInstructionTable* eit);


FENNEL_END_NAMESPACE

#endif

// End ExtMath.h
