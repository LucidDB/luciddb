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


#include "fennel/common/CommonPreamble.h"
#include "fennel/calc/ExtMath.h"
#include "fennel/calc/ExtendedInstructionTable.h"
#include <math.h>

FENNEL_BEGIN_NAMESPACE


void
calcLn(RegisterRef<double>* result,
         RegisterRef<double>* x)
{
    assert(x->type() == STANDARD_TYPE_DOUBLE || x->type() == STANDARD_TYPE_REAL);

    if (x->isNull()) {
        result->toNull();        
    } else {
        result->value(log(x->value())); //using the c math library log
    }
}

void
calcLog10(RegisterRef<double>* result,
         RegisterRef<double>* x)
{
    assert(x->type() == STANDARD_TYPE_DOUBLE || x->type() == STANDARD_TYPE_REAL);

    if (x->isNull()) {
        result->toNull();        
    } else {
        result->value(log10(x->value()));
    }
}

void
calcAbs(RegisterRef<double>* result,
	RegisterRef<double>* x)
{
    assert(x->type() == STANDARD_TYPE_DOUBLE || x->type() == STANDARD_TYPE_REAL);

    if (x->isNull()) {
        result->toNull();        
    } else {
        result->value(fabs(x->value()));
    }
}

void
calcAbs(RegisterRef<long long>* result,
	RegisterRef<long long>* x)
{
    assert(x->type() == STANDARD_TYPE_INT_64);

    if (x->isNull()) {
        result->toNull();        
    } else {
        result->value(labs(x->value()));
    }
}

void
calcPow(RegisterRef<double>* result,
         RegisterRef<double>* x,
         RegisterRef<double>* y)
{
    assert(x->type() == STANDARD_TYPE_DOUBLE || x->type() == STANDARD_TYPE_REAL);
    assert(y->type() == STANDARD_TYPE_DOUBLE || y->type() == STANDARD_TYPE_REAL);

    if (x->isNull() || y->isNull()) {
        result->toNull();        
    } else {
        result->value(pow(x->value(), y->value()));
    }
}

void
calcMod(RegisterRef<long long>* result,
	RegisterRef<long long>* x,
	RegisterRef<long long>* y)
{
  assert(x->type() == STANDARD_TYPE_INT_64);
  assert(y->type() == STANDARD_TYPE_INT_64);

    if (x->isNull() || y->isNull()) {
        result->toNull();        
    } else {
        //REVIEW wael: need to check for divide by zero here
        result->value(x->value() % y->value());
    }
}


void
ExtMathRegister(ExtendedInstructionTable* eit)
{
    assert(eit != NULL);
    
    vector<StandardTypeDescriptorOrdinal> params_2D;
    params_2D.push_back(STANDARD_TYPE_DOUBLE);
    params_2D.push_back(STANDARD_TYPE_DOUBLE);

    vector<StandardTypeDescriptorOrdinal> params_3D(params_2D);
    params_3D.push_back(STANDARD_TYPE_DOUBLE);

    vector<StandardTypeDescriptorOrdinal> params_2I;
    params_2I.push_back(STANDARD_TYPE_INT_64);
    params_2I.push_back(STANDARD_TYPE_INT_64);

    vector<StandardTypeDescriptorOrdinal> params_3I(params_2I);
    params_3I.push_back(STANDARD_TYPE_INT_64);

    eit->add("LN", params_2D,
             (ExtendedInstruction2<double, double>*) NULL,
             &calcLn);

    eit->add("LOG10", params_2D,
             (ExtendedInstruction2<double, double>*) NULL,
             &calcLog10);
    
    eit->add("ABS", params_2D,
             (ExtendedInstruction2<double, double>*) NULL,
             &calcAbs);
    
    eit->add("ABS", params_2I,
             (ExtendedInstruction2<long long, long long>*) NULL,
             &calcAbs);

    eit->add("POW", params_3D,
             (ExtendedInstruction3<double, double, double>*) NULL,
             &calcPow);

    eit->add("MOD", params_3I,
             (ExtendedInstruction3<long long, long long, long long>*) NULL,
             &calcMod);

}


FENNEL_END_NAMESPACE

        
