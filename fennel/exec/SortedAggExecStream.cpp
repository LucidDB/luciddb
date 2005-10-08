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
#include "fennel/exec/SortedAggExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SortedAggExecStream::prepare(SortedAggExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    
    inputTuple.compute(pInAccessor->getTupleDesc());
    
    TupleDescriptor outputDesc;
    TupleDescriptor const &inputDesc = pInAccessor->getTupleDesc();

    // Attribute descriptor for COUNT output
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor countDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    
    // Compute outputDesc based on requested aggregate function invocations,
    // and instantiate polymorphic AggComputers bound to correct inputs.
    for (AggInvocationConstIter pInvocation(params.aggInvocations.begin());
         pInvocation != params.aggInvocations.end();
         ++pInvocation)
    {
        switch(pInvocation->aggFunction) {
        case AGG_FUNC_COUNT:
            outputDesc.push_back(countDesc);
            break;
        case AGG_FUNC_SUM:
        case AGG_FUNC_MIN:
        case AGG_FUNC_MAX:
            // Output type is same as input type, but nullable
            outputDesc.push_back(inputDesc[pInvocation->iInputAttr]);
            outputDesc.back().isNullable = true;
            break;
        }
        TupleAttributeDescriptor const *pInputAttr = NULL;
        if (pInvocation->iInputAttr != -1) {
            pInputAttr = &(inputDesc[pInvocation->iInputAttr]);
        }
        aggComputers.push_back(
            AggComputer::newAggComputer(
                pInvocation->aggFunction,
                pInputAttr));
        aggComputers.back().setInputAttrIndex(pInvocation->iInputAttr);
    }

    // Sanity check:  the output shape we computed should agree with
    // the descriptor (if any) in the supplied plan.
    if (!params.outputTupleDesc.empty()) {
        assert(outputDesc == params.outputTupleDesc);
    }

    accumulatorTuple.computeAndAllocate(outputDesc);
    outputTuple.compute(outputDesc);
    pOutAccessor->setTupleShape(outputDesc);
}

inline void SortedAggExecStream::clearAccumulator()
{
    for (int i = 0; i < aggComputers.size(); ++i) {
        aggComputers[i].clearAccumulator(accumulatorTuple[i]);
    }
}

inline void SortedAggExecStream::updateAccumulator()
{
    for (int i = 0; i < aggComputers.size(); ++i) {
        aggComputers[i].updateAccumulator(accumulatorTuple[i], inputTuple);
    }
}

inline void SortedAggExecStream::computeOutput()
{
    for (int i = 0; i < aggComputers.size(); ++i) {
        aggComputers[i].computeOutput(outputTuple[i], accumulatorTuple[i]);
    }
}

void SortedAggExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    clearAccumulator();
    state = STATE_ACCUMULATING;
}

ExecStreamResult SortedAggExecStream::execute(ExecStreamQuantum const &quantum)
{
    if (pInAccessor->getState() == EXECBUF_EOS) {
        // no more input is coming
        if (state == STATE_DONE) {
            // already produced output
            pOutAccessor->markEOS();
            return EXECRC_EOS;
        }
        if (state == STATE_ACCUMULATING) {
            // compute final output and get ready to write it
            computeOutput();
            state = STATE_PRODUCING;
        }
        // attempt to write output
        bool success = pOutAccessor->produceTuple(outputTuple);
        if (success) {
            state = STATE_DONE;
            // let precheckConduitBuffers below return EOS for us
        } else {
            return EXECRC_BUF_OVERFLOW;
        }
    }

    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    for (uint nTuples = 0; nTuples < quantum.nTuplesMax; ++nTuples) {
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }
        pInAccessor->unmarshalTuple(inputTuple);
        updateAccumulator();
        pInAccessor->consumeTuple();
    }

    return EXECRC_YIELD;
}

FENNEL_END_CPPFILE("$Id$");

// End SortedAggExecStream.cpp
