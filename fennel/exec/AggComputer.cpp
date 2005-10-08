/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
#include "fennel/exec/AggComputer.h"
#include "fennel/exec/AggComputerImpl.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

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
        switch(pAttrDesc->pTypeDescriptor->getOrdinal()) {
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
        assert(pAttrDesc);
        return new ExtremeAggComputer(*pAttrDesc, aggFunction == AGG_FUNC_MIN);
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

ExtremeAggComputer::ExtremeAggComputer(
    TupleAttributeDescriptor const &attrDesc,
    bool isMinInit)
{
    pTypeDescriptor = attrDesc.pTypeDescriptor;
    isMin = isMinInit;
}

void ExtremeAggComputer::clearAccumulator(TupleDatum &accumulatorDatum)
{
    isResultNull = true;
}

inline void ExtremeAggComputer::copyInputToAccumulator(
    TupleDatum &accumulatorDatum, 
    TupleDatum const &inputDatum)
{
    // TODO jvs 7-Oct-2005:  make this a TupleDatum utility function
    memcpy(
        const_cast<PBuffer>(accumulatorDatum.pData),
        inputDatum.pData,
        inputDatum.cbData);
    accumulatorDatum.cbData = inputDatum.cbData;
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
    }
    // c = (input - accumulator)
    int c = pTypeDescriptor->compareValues(
        inputDatum.pData,
        inputDatum.cbData,
        accumulatorDatum.pData,
        accumulatorDatum.cbData);
    if (isMin) {
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

FENNEL_END_CPPFILE("$Id$");

// End AggComputer.cpp
