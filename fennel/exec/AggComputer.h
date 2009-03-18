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

#ifndef Fennel_AggComputer_Included
#define Fennel_AggComputer_Included

#include <boost/ptr_container/ptr_vector.hpp>
#include "fennel/common/FemEnums.h"

FENNEL_BEGIN_NAMESPACE

class StoredTypeDescriptor;
class TupleAttributeDescriptor;
class TupleData;
class TupleDatum;

/**
 * Abstract base class representing computation of a single aggregate function
 * over a collection of scalar values all having the same group key.  In order
 * to be used for more complex functions such as AVG which require compound
 * accumulator state, it needs to be extended to allow AggComputer to
 * collaborate with its caller in defining the accumulator tuple.
 * Luckily, Farrago rewrites AVG to SUM/COUNT.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class AggComputer
{
protected:
    int iInputAttr;

public:
    explicit AggComputer();

    virtual ~AggComputer();

    /**
     * Virtual constructor.
     *
     * @param aggFunction function for which to construct a computer
     *
     * @param pAttrDesc descriptor for input attribute, or NULL
     * for no input attribute (as in COUNT(*))
     */
    static AggComputer *newAggComputer(
        AggFunction aggFunction,
        TupleAttributeDescriptor const *pAttrDesc);

    /**
     * Sets the attribute index from which this computer should read
     * input values in source tuples.
     *
     * @param iInputAttrIndex 0-based tuple attribute index
     */
    virtual void setInputAttrIndex(uint iInputAttrIndex);

    /**
     * Clears an accumulator.
     *
     * @param accumulatorDatum in-memory value to be cleared
     */
    virtual void clearAccumulator(
        TupleDatum &accumulatorDatum) = 0;

    /**
     * Updates an accumulator with a new input tuple.
     *
     * @param accumulatorDatum in-memory value to be updated
     *
     * @param inputTuple source for update; no references to this
     * data should be retained after this method returns
     */
    virtual void updateAccumulator(
        TupleDatum &accumulatorDatum,
        TupleData const &inputTuple) = 0;

    /**
     * Computes an output based on accumulator state.
     *
     * @param outputDatum receives reference to computed output in preparation
     * for marshalling result
     *
     * @param accumulatorDatum final in-memory accumulator state
     */
    virtual void computeOutput(
        TupleDatum &outputDatum,
        TupleDatum const &accumulatorDatum) = 0;

    /**
     * Initializes a new accumulator datum from an input tuple.
     *
     * @param accumulatorDatumDest in-memory value to be updated. Memory needs
     * to be associated with this datum by the caller.
     *
     * @param inputTuple source for update; no references to this
     * data should be retained after this method returns
     */
    virtual void initAccumulator(
        TupleDatum &accumulatorDatumDest,
        TupleData const &inputTuple) = 0;

    /**
     * Initializes a new accumulator datum from an existing accumulator datum.
     *
     * @param accumulatorDatumSrc the existing accumulator datum
     * @param accumulatorDatumDest the new accumulator datum. Memory needs to
     * be associated with this datum by the caller.
     */
    virtual void initAccumulator(
        TupleDatum &accumulatorDatumSrc,
        TupleDatum &accumulatorDatumDest) = 0;

    /**
     * Computes a new accumulator from an existing accumulator dataum and a new
     * input tuple.
     *
     * @param accumulatorDatumSrc the existing accumulator datum
     * @param accumulatorDatumDest the new accumulator datum. memory needs to
     * be associated with this datum by the caller.
     * @param inputTuple source for update; no references to this
     * data should be retained after this method returns
     */
    virtual void updateAccumulator(
        TupleDatum &accumulatorDatumSrc,
        TupleDatum &accumulatorDatumDest,
        TupleData const &inputTuple) = 0;
};

typedef boost::ptr_vector<AggComputer> AggComputerList;
typedef boost::ptr_vector<AggComputer>::iterator AggComputerIter;
typedef boost::ptr_vector<AggComputer>::const_iterator AggComputerConstIter;

FENNEL_END_NAMESPACE

#endif

// End AggComputer.h
