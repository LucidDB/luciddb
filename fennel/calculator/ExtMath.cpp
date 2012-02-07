/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/


#include "fennel/common/CommonPreamble.h"
#include "fennel/calculator/ExtMath.h"
#include "fennel/calculator/ExtendedInstructionTable.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include <cstdlib>   // for std::abs()
#include <math.h>

FENNEL_BEGIN_NAMESPACE


void
mathLn(
    RegisterRef<double>* result,
    RegisterRef<double>* x)
{
    assert(StandardTypeDescriptor::isApprox(x->type()));

    if (x->isNull()) {
        result->toNull();
    } else if (x->value() <= 0.0) {
        result->toNull();
        // Invalid Argument For Natural Logarithm
        throw SqlState::instance().code2201E();
    } else {
        result->value(log(x->value())); //using the c math library log
    }
}

void
mathLn(
    RegisterRef<double>* result,
    RegisterRef<long long>* x)
{
    assert(StandardTypeDescriptor::isExact(x->type()));

    if (x->isNull()) {
        result->toNull();
    } else if (x->value() <= 0) {
        result->toNull();
        // Invalid Argument For Natural Logarithm
        throw SqlState::instance().code2201E();
    } else {
        result->value(log(double(x->value()))); //using the c math library log
    }
}

void
mathLog10(
    RegisterRef<double>* result,
    RegisterRef<double>* x)
{
    assert(StandardTypeDescriptor::isApprox(x->type()));

    if (x->isNull()) {
        result->toNull();
    } else if (x->value() <= 0.0) {
        result->toNull();
        // Invalid Argument For Natural Logarithm
        throw SqlState::instance().code2201E();
    } else {
        result->value(log10(x->value()));
    }
}

void
mathLog10(
    RegisterRef<double>* result,
    RegisterRef<long long>* x)
{
    assert(StandardTypeDescriptor::isExact(x->type()));

    if (x->isNull()) {
        result->toNull();
    } else if (x->value() <= 0) {
        result->toNull();
        // Invalid Argument For Natural Logarithm
        throw SqlState::instance().code2201E();
    } else {
        result->value(log10(double(x->value())));
    }
}

void
mathAbs(
    RegisterRef<double>* result,
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
mathAbs(
    RegisterRef<long long>* result,
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
mathPow(
    RegisterRef<double>* result,
    RegisterRef<double>* x,
    RegisterRef<double>* y)
{
    assert(StandardTypeDescriptor::isApprox(x->type()));
    assert(StandardTypeDescriptor::isApprox(y->type()));

    if (x->isNull() || y->isNull()) {
        result->toNull();
    } else {
        double r = pow(x->value(), y->value());
        if ((x->value() == 0.0 && y->value() < 0.0)
            || (x->value() <  0.0 && isnan(r)))
        {
            // we should get here when x^y have
            // x=0 AND y < 0 OR
            // x<0 AND y is an non integer.
            // If this is the case then the result is NaN

            result->toNull();
            // Data Exception - Invalid Argument For Power Function
            throw SqlState::instance().code2201F();

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

    eit->add(
        "LN", params_2D,
        (ExtendedInstruction2<double, double>*) NULL,
        &mathLn);

    eit->add(
        "LN", params_DI,
        (ExtendedInstruction2<double, long long>*) NULL,
        &mathLn);

    eit->add(
        "LOG10", params_2D,
        (ExtendedInstruction2<double, double>*) NULL,
        &mathLog10);

    eit->add(
        "LOG10", params_DI,
        (ExtendedInstruction2<double, long long>*) NULL,
        &mathLog10);

    eit->add(
        "ABS", params_2D,
        (ExtendedInstruction2<double, double>*) NULL,
        &mathAbs);

    eit->add(
        "ABS", params_2I,
        (ExtendedInstruction2<long long, long long>*) NULL,
        &mathAbs);

    eit->add(
        "POW", params_3D,
        (ExtendedInstruction3<double, double, double>*) NULL,
        &mathPow);

}


FENNEL_END_NAMESPACE

// End ExtMath.cpp
