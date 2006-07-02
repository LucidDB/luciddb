/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/lucidera/bitmap/LbmSortedAggExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/lucidera/bitmap/LbmByteSegment.h"
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
    virtual void clearAccumulator(
        TupleDatum &accumulatorDatum);

    virtual void updateAccumulator(
        TupleDatum &accumulatorDatum,
        TupleData const &inputTuple);

    virtual void computeOutput(
        TupleDatum &outputDatum,
        TupleDatum const &accumulatorDatum);

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
    pComputer->initAccumulator(accumulatorDatumDest, inputTuple);
}

void LbmRepeatingAggComputer::initAccumulator(
    TupleDatum &accumulatorDatumSrc,
    TupleDatum &accumulatorDatumDest)
{
    pComputer->initAccumulator(accumulatorDatumSrc, accumulatorDatumDest);
}

void LbmRepeatingAggComputer::updateAccumulator(
    TupleDatum &accumulatorDatumSrc,
    TupleDatum &accumulatorDatumDest,
    TupleData const &inputTuple)
{
    // FIXME: this sequence seems questionable since it does not follow
    // the pattern of SUM aggregates. SUM aggregates seem to perform
    // special case copies. (because the source datum is nullable?)
    // yet this sequence seems reasonable since no allocated memory is
    // associated with the datums
    updateAccumulator(accumulatorDatumSrc, inputTuple);
    accumulatorDatumDest.copyFrom(accumulatorDatumSrc);
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
