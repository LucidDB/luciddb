/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
#include "fennel/common/FennelResource.h"
#include "fennel/exec/AggComputerImpl.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/common/FennelExcn.h"

FENNEL_BEGIN_CPPFILE("$Id$");

using namespace std;

AggComputer *AggComputer::newAggComputer(
    AggFunction aggFunction,
    TupleAttributeDescriptor const *pAttrDesc)
{
    switch (aggFunction) {
    case AGG_FUNC_COUNT:
        if (pAttrDesc) {
            return new CountNullableAggComputer();
        } else {
            return new CountStarAggComputer();
        }
    case AGG_FUNC_SUM:
        assert(pAttrDesc);
        // TODO jvs 6-Oct-2005:  gotta use some of that template
        // metaprogramming mumbo jumbo to get rid of this
        switch (pAttrDesc->pTypeDescriptor->getOrdinal()) {
        case STANDARD_TYPE_INT_8:
            return new SumAggComputer<int8_t>();
        case STANDARD_TYPE_UINT_8:
            return new SumAggComputer<uint8_t>();
        case STANDARD_TYPE_INT_16:
            return new SumAggComputer<int16_t>();
        case STANDARD_TYPE_UINT_16:
            return new SumAggComputer<uint16_t>();
        case STANDARD_TYPE_INT_32:
            return new SumAggComputer<int32_t>();
        case STANDARD_TYPE_UINT_32:
            return new SumAggComputer<uint32_t>();
        case STANDARD_TYPE_INT_64:
            return new SumAggComputer<int64_t>();
        case STANDARD_TYPE_UINT_64:
            return new SumAggComputer<uint64_t>();
        case STANDARD_TYPE_REAL:
            return new SumAggComputer<float>();
        case STANDARD_TYPE_DOUBLE:
            return new SumAggComputer<double>();
        }
    case AGG_FUNC_MIN:
    case AGG_FUNC_MAX:
    case AGG_FUNC_SINGLE_VALUE:
        assert(pAttrDesc);
        return new ExtremeAggComputer(aggFunction, *pAttrDesc);
    }
    permAssert(false);
}

AggComputer::AggComputer()
{
    iInputAttr = -1;
}

void AggComputer::setInputAttrIndex(uint iInputAttrInit)
{
    iInputAttr = iInputAttrInit;
}

AggComputer::~AggComputer()
{
}

inline uint64_t &CountAggComputer::interpretDatum(TupleDatum &datum)
{
    assert(datum.cbData == sizeof(uint64_t));
    assert(datum.pData);
    return *reinterpret_cast<uint64_t *>(const_cast<PBuffer>(datum.pData));
}

inline void CountAggComputer::clearAccumulatorImpl(TupleDatum &accumulatorDatum)
{
    uint64_t &count = interpretDatum(accumulatorDatum);
    count = 0;
}

inline void CountAggComputer::initAccumulatorImpl(TupleDatum &accumulatorDatum)
{
    uint64_t &count = interpretDatum(accumulatorDatum);
    count = 1;
}

void CountAggComputer::updateAccumulatorImpl(
    TupleDatum &accumulatorDatum)
{
    uint64_t &count = interpretDatum(accumulatorDatum);
    ++count;
}

void CountAggComputer::computeOutput(
    TupleDatum &outputDatum,
    TupleDatum const &accumulatorDatum)
{
    // Set output to alias accumulator value directly.
    outputDatum = accumulatorDatum;
}

void CountStarAggComputer::clearAccumulator(TupleDatum &accumulatorDatum)
{
    assert(iInputAttr == -1);
    clearAccumulatorImpl(accumulatorDatum);
}

void CountStarAggComputer::updateAccumulator(
    TupleDatum &accumulatorDatum,
    TupleData const &)
{
    updateAccumulatorImpl(accumulatorDatum);
}

void CountStarAggComputer::initAccumulator(
    TupleDatum &accumulatorDatum,
    TupleData const &)
{
    assert(iInputAttr == -1);
    initAccumulatorImpl(accumulatorDatum);
}

void CountStarAggComputer::initAccumulator(
    TupleDatum &accumulatorDatumSrc,
    TupleDatum &accumulatorDatumDest)
{
    accumulatorDatumDest.memCopyFrom(accumulatorDatumSrc);
}

void CountStarAggComputer::updateAccumulator(
    TupleDatum &accumulatorDatumSrc,
    TupleDatum &accumulatorDatumDest,
    TupleData const &)
{
    updateAccumulatorImpl(accumulatorDatumSrc);
    /*
     * For count, accumulatorDatumSrc can accomodate the updated value so
     * there is no need to use memCopyFrom.
     */
    accumulatorDatumDest.copyFrom(accumulatorDatumSrc);
}

void CountNullableAggComputer::clearAccumulator(TupleDatum &accumulatorDatum)
{
    clearAccumulatorImpl(accumulatorDatum);
}

void CountNullableAggComputer::updateAccumulator(
    TupleDatum &accumulatorDatum,
    TupleData const &inputTuple)
{
    assert(iInputAttr != -1);
    TupleDatum const &inputDatum = inputTuple[iInputAttr];
    if (inputDatum.pData) {
        updateAccumulatorImpl(accumulatorDatum);
    }
}

void CountNullableAggComputer::initAccumulator(
    TupleDatum &accumulatorDatum,
    TupleData const &inputTuple)
{
    assert(iInputAttr != -1);
    TupleDatum const &inputDatum = inputTuple[iInputAttr];
    if (inputDatum.pData) {
        initAccumulatorImpl(accumulatorDatum);
    } else {
        clearAccumulatorImpl(accumulatorDatum);
    }
}

void CountNullableAggComputer::initAccumulator(
    TupleDatum &accumulatorDatumSrc,
    TupleDatum &accumulatorDatumDest)
{
    accumulatorDatumDest.memCopyFrom(accumulatorDatumSrc);
}

void CountNullableAggComputer::updateAccumulator(
    TupleDatum &accumulatorDatumSrc,
    TupleDatum &accumulatorDatumDest,
    TupleData const &inputTuple)
{
    assert(iInputAttr != -1);
    TupleDatum const &inputDatum = inputTuple[iInputAttr];
    if (inputDatum.pData) {
        updateAccumulatorImpl(accumulatorDatumSrc);
    }
    /*
     * For count, accumulatorDatumSrc can accomodate the updated value so
     * there is no need to use memCopyFrom.
     */
    accumulatorDatumDest.copyFrom(accumulatorDatumSrc);
}

ExtremeAggComputer::ExtremeAggComputer(
    AggFunction aggFunctionInit,
    TupleAttributeDescriptor const &attrDesc)
{
    aggFunction = aggFunctionInit;
    pTypeDescriptor = attrDesc.pTypeDescriptor;
}

void ExtremeAggComputer::clearAccumulator(TupleDatum &accumulatorDatum)
{
    isResultNull = true;
}

inline void ExtremeAggComputer::copyInputToAccumulator(
    TupleDatum &accumulatorDatum,
    TupleDatum const &inputDatum)
{
    // Use the utility function to copy from inputDatum's buffer to
    // accumulatorDatum.
    accumulatorDatum.memCopyFrom(inputDatum);
}

void ExtremeAggComputer::updateAccumulator(
    TupleDatum &accumulatorDatum,
    TupleData const &inputTuple)
{
    assert(iInputAttr != -1);
    TupleDatum const &inputDatum = inputTuple[iInputAttr];
    if (!inputDatum.pData) {
        // SQL2003 Part 2 Section 10.9 General Rule 4.a
        // TODO jvs 6-Oct-2005:  we're supposed to queue a warning
        // for null value eliminated in set function
        return;
    }
    if (isResultNull) {
        isResultNull = false;
        // first non-null input:  use it
        copyInputToAccumulator(accumulatorDatum, inputDatum);
        return;
    } else if (aggFunction == AGG_FUNC_SINGLE_VALUE) {
        throw FennelExcn(
            FennelResource::instance().scalarQueryReturnedMultipleRows());
    }

    // c = (input - accumulator)
    int c = pTypeDescriptor->compareValues(
        inputDatum.pData,
        inputDatum.cbData,
        accumulatorDatum.pData,
        accumulatorDatum.cbData);
    if (aggFunction == AGG_FUNC_MIN) {
        // invert comparison for MIN
        c = -c;
    }
    if (c <= 0) {
        // for MAX, input has to be greater than accumulator for accumulator
        // to be updated
        return;
    }
    copyInputToAccumulator(accumulatorDatum, inputDatum);
}

void ExtremeAggComputer::computeOutput(
    TupleDatum &outputDatum,
    TupleDatum const &accumulatorDatum)
{
    // Set output to alias accumulator value directly.
    outputDatum = accumulatorDatum;
    if (isResultNull) {
        outputDatum.pData = NULL;
    }
}

void ExtremeAggComputer::initAccumulator(
    TupleDatum &accumulatorDatumDest,
    TupleData const &inputTuple)
{
    accumulatorDatumDest.memCopyFrom(inputTuple[iInputAttr]);
    isResultNull = false;
}

void ExtremeAggComputer::initAccumulator(
    TupleDatum &accumulatorDatumSrc,
    TupleDatum &accumulatorDatumDest)
{
    accumulatorDatumDest.memCopyFrom(accumulatorDatumSrc);
    isResultNull = false;
}

void ExtremeAggComputer::updateAccumulator(
    TupleDatum &accumulatorDatumSrc,
    TupleDatum &accumulatorDatumDest,
    TupleData const &inputTuple)
{
    if (aggFunction == AGG_FUNC_SINGLE_VALUE) {
        throw FennelExcn(
            FennelResource::instance().scalarQueryReturnedMultipleRows());
    }

    TupleDatum const &inputDatum = inputTuple[iInputAttr];

    if (!accumulatorDatumSrc.pData) {
        accumulatorDatumDest.memCopyFrom(inputDatum);
    } else if (inputDatum.pData) {
        // c = (input - accumulator)
        int c = pTypeDescriptor->compareValues(
            inputDatum.pData,
            inputDatum.cbData,
            accumulatorDatumSrc.pData,
            accumulatorDatumSrc.cbData);
        if (aggFunction == AGG_FUNC_MIN) {
            // invert comparison for MIN
            c = -c;
        }
        if (c <= 0) {
            // for MAX, input has to be greater than accumulator for accumulator
            // to be updated
            accumulatorDatumDest.memCopyFrom(accumulatorDatumSrc);
        } else {
            accumulatorDatumDest.memCopyFrom(inputDatum);
        }
    } else {
        accumulatorDatumDest.memCopyFrom(accumulatorDatumSrc);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End AggComputer.cpp
