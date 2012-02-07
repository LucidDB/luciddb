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
#include "fennel/lbm/LbmSortedAggExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/lbm/LbmByteSegment.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

/**
 * LbmRepeatingAggComputer is an aggregate computer that wraps another
 * aggregate computer. Its input is expected to be bitmap tuples. On an
 * update, it counts bits from the segment data field (expected to be
 * the last one) and applies the update multiple times, once for each bit
 * set in the segment data field.
 */
class LbmRepeatingAggComputer : public AggComputer
{
    AggComputer *pComputer;

public:
    explicit LbmRepeatingAggComputer(AggComputer *pComputer);

    // implement AggComputer
    virtual void setInputAttrIndex(uint iInputAttrIndex);

    virtual void clearAccumulator(
        TupleDatum &accumulatorDatum);

    virtual void updateAccumulator(
        TupleDatum &accumulatorDatum,
        TupleData const &inputTuple);

    virtual void computeOutput(
        TupleDatum &outputDatum,
        TupleDatum const &accumulatorDatum);

    // unused...
    virtual void initAccumulator(
        TupleDatum &accumulatorDatumDest,
        TupleData const &inputTuple);

    virtual void initAccumulator(
        TupleDatum &accumulatorDatumSrc,
        TupleDatum &accumulatorDatumDest);

    virtual void updateAccumulator(
        TupleDatum &accumulatorDatumSrc,
        TupleDatum &accumulatorDatumDest,
        TupleData const &inputTuple);
};

LbmRepeatingAggComputer::LbmRepeatingAggComputer(
    AggComputer *pComputer)
{
    this->pComputer = pComputer;
}

void LbmRepeatingAggComputer::setInputAttrIndex(uint iInputAttrIndex)
{
    AggComputer::setInputAttrIndex(iInputAttrIndex);
    pComputer->setInputAttrIndex(iInputAttrIndex);
}

void LbmRepeatingAggComputer::clearAccumulator(
    TupleDatum &accumulatorDatum)
{
    pComputer->clearAccumulator(accumulatorDatum);
}

void LbmRepeatingAggComputer::updateAccumulator(
    TupleDatum &accumulatorDatum,
    TupleData const &inputTuple)
{
    // segment data should be contained in the last field
    TupleDatum segmentDatum = inputTuple[inputTuple.size() - 1];
    uint loops = LbmByteSegment::countBits(segmentDatum);

    for (uint i = 0; i < loops; i++) {
        pComputer->updateAccumulator(accumulatorDatum, inputTuple);
    }
}

void LbmRepeatingAggComputer::computeOutput(
    TupleDatum &outputDatum,
    TupleDatum const &accumulatorDatum)
{
    pComputer->computeOutput(outputDatum, accumulatorDatum);
}

void LbmRepeatingAggComputer::initAccumulator(
    TupleDatum &accumulatorDatumDest,
    TupleData const &inputTuple)
{
    // sorted aggregates never use this call
    assert(false);
}

void LbmRepeatingAggComputer::initAccumulator(
    TupleDatum &accumulatorDatumSrc,
    TupleDatum &accumulatorDatumDest)
{
    // sorted aggregates never use this call
    assert(false);
}

void LbmRepeatingAggComputer::updateAccumulator(
    TupleDatum &accumulatorDatumSrc,
    TupleDatum &accumulatorDatumDest,
    TupleData const &inputTuple)
{
    // sorted aggregates never use this call
    assert(false);
}

void LbmSortedAggExecStream::prepare(
    LbmSortedAggExecStreamParams const &params)
{
    SortedAggExecStream::prepare(params);
}

AggComputer *LbmSortedAggExecStream::newAggComputer(
    AggFunction aggFunction,
    TupleAttributeDescriptor const *pAttrDesc)
{
    AggComputer *pComputer =
        SortedAggExecStream::newAggComputer(aggFunction, pAttrDesc);

    switch (aggFunction) {
    case AGG_FUNC_COUNT:
    case AGG_FUNC_SUM:
        pComputer = new LbmRepeatingAggComputer(pComputer);
    default:
        ;
    }
    return pComputer;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmSortedAggExecStream.cpp
