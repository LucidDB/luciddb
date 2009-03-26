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


#include "fennel/common/CommonPreamble.h"
#include "fennel/calculator/ExtMath.h"
#include "fennel/calculator/ExtendedInstructionTable.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include <cstdlib>   // for std::abs()
#include <math.h>

FENNEL_BEGIN_NAMESPACE


void
mathLn(RegisterRef<double>* result,
       RegisterRef<double>* x)
{
    assert(StandardTypeDescriptor::isApprox(x->type()));

    if (x->isNull()) {
        result->toNull();
    } else if (x->value() <= 0.0) {
        result->toNull();
        // SQL99 Part 2 Section 22.1 22-023 "invalid parameter value"
        throw "22023";
    }else {
        result->value(log(x->value())); //using the c math library log
    }
}

void
mathLn(RegisterRef<double>* result,
       RegisterRef<long long>* x)
{
    assert(StandardTypeDescriptor::isExact(x->type()));

    if (x->isNull()) {
        result->toNull();
    } else if (x->value() <= 0) {
        result->toNull();
        // SQL99 Part 2 Section 22.1 22-023 "invalid parameter value"
        throw "22023";
    }else {
        result->value(log(double(x->value()))); //using the c math library log
    }
}

void
mathLog10(RegisterRef<double>* result,
      RegisterRef<double>* x)
{
    assert(StandardTypeDescriptor::isApprox(x->type()));

    if (x->isNull()) {
        result->toNull();
    } else if (x->value() <= 0.0) {
        result->toNull();
        // SQL99 Part 2 Section 22.1 22-023 "invalid parameter value"
        throw "22023";
    } else {
        result->value(log10(x->value()));
    }
}

void
mathLog10(RegisterRef<double>* result,
      RegisterRef<long long>* x)
{
    assert(StandardTypeDescriptor::isExact(x->type()));

    if (x->isNull()) {
        result->toNull();
    } else if (x->value() <= 0) {
        result->toNull();
        // SQL99 Part 2 Section 22.1 22-023 "invalid parameter value"
        throw "22023";
    } else {
        result->value(log10(double(x->value())));
    }
}

void
mathAbs(RegisterRef<double>* result,
    RegisterRef<double>* x)
{
    assert(StandardTypeDescriptor::isApprox(x->type()));

    if (x->isNull()) {
        result->toNull();
    } else {
        result->value(fabs(x->value()));
    }
}

void
mathAbs(RegisterRef<long long>* result,
        RegisterRef<long long>* x)
{
    assert(x->type() == STANDARD_TYPE_INT_64);

    if (x->isNull()) {
        result->toNull();
    } else {
        // Due to various include problems with gcc, it's easy to get
        // abs doubly defined. Just arbitrarily using std::abs to
        // avoid problems with gcc3.x built-ins.
        result->value(std::abs(x->value()));
    }
}

void
mathPow(RegisterRef<double>* result,
    RegisterRef<double>* x,
    RegisterRef<double>* y)
{
    assert(StandardTypeDescriptor::isApprox(x->type()));
    assert(StandardTypeDescriptor::isApprox(y->type()));

    if (x->isNull() || y->isNull()) {
        result->toNull();
    } else {
        double r = pow(x->value(), y->value());
        if ( (x->value() == 0.0 && y->value() < 0.0) ||
            (x->value() <  0.0 && isnan(r))
            ) {
            //we should get here when x^y have
            //x=0 AND y < 0 OR
            //x<0 AND y is an non integer. If this is the case then the result is NaN

            result->toNull();
            // SQL99 Part 2 Section 22.1 22-023 "invalid parameter value"
            throw "22023";

        } else {
            result->value(r);
        }
    }
}

void
ExtMathRegister(ExtendedInstructionTable* eit)
{
    assert(eit != NULL);

    vector<StandardTypeDescriptorOrdinal> params_2D;
    params_2D.push_back(STANDARD_TYPE_DOUBLE);
    params_2D.push_back(STANDARD_TYPE_DOUBLE);

    vector<StandardTypeDescriptorOrdinal> params_DI;
    params_DI.push_back(STANDARD_TYPE_DOUBLE);
    params_DI.push_back(STANDARD_TYPE_INT_64);

    vector<StandardTypeDescriptorOrdinal> params_DII;
    params_DII.push_back(STANDARD_TYPE_DOUBLE);
    params_DII.push_back(STANDARD_TYPE_INT_64);
    params_DII.push_back(STANDARD_TYPE_INT_64);

    vector<StandardTypeDescriptorOrdinal> params_DID;
    params_DID.push_back(STANDARD_TYPE_DOUBLE);
    params_DID.push_back(STANDARD_TYPE_INT_64);
    params_DID.push_back(STANDARD_TYPE_DOUBLE);

    vector<StandardTypeDescriptorOrdinal> params_DDI;
    params_DDI.push_back(STANDARD_TYPE_DOUBLE);
    params_DDI.push_back(STANDARD_TYPE_DOUBLE);
    params_DDI.push_back(STANDARD_TYPE_INT_64);


    vector<StandardTypeDescriptorOrdinal> params_3D(params_2D);
    params_3D.push_back(STANDARD_TYPE_DOUBLE);

    vector<StandardTypeDescriptorOrdinal> params_2I;
    params_2I.push_back(STANDARD_TYPE_INT_64);
    params_2I.push_back(STANDARD_TYPE_INT_64);

    vector<StandardTypeDescriptorOrdinal> params_3I(params_2I);
    params_3I.push_back(STANDARD_TYPE_INT_64);

    eit->add("LN", params_2D,
             (ExtendedInstruction2<double, double>*) NULL,
             &mathLn);

    eit->add("LN", params_DI,
             (ExtendedInstruction2<double, long long>*) NULL,
             &mathLn);

    eit->add("LOG10", params_2D,
             (ExtendedInstruction2<double, double>*) NULL,
             &mathLog10);

    eit->add("LOG10", params_DI,
             (ExtendedInstruction2<double, long long>*) NULL,
             &mathLog10);

    eit->add("ABS", params_2D,
             (ExtendedInstruction2<double, double>*) NULL,
             &mathAbs);

    eit->add("ABS", params_2I,
             (ExtendedInstruction2<long long, long long>*) NULL,
             &mathAbs);

    eit->add("POW", params_3D,
             (ExtendedInstruction3<double, double, double>*) NULL,
             &mathPow);

}


FENNEL_END_NAMESPACE

// End ExtMath.cpp
