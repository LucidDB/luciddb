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
#include "fennel/exec/SortedAggExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SortedAggExecStream::prepare(SortedAggExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    inputTuple.compute(pInAccessor->getTupleDesc());

    /*
      prevTuple contains the groupByKey fields as well as the accumulator
      result fields. outputTuple has the same format as prevTuple.
      The difference is that prevTuple has buffer associated with it while
      outputTuple has pointers pointing to the result location.
    */
    TupleDescriptor prevTupleDesc;
    TupleDescriptor const &inputDesc = pInAccessor->getTupleDesc();

    // Attribute descriptor for COUNT output
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor countDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    groupByKeyCount = params.groupByKeyCount;

    for (int i = 0; i < groupByKeyCount; i ++) {
        prevTupleDesc.push_back(inputDesc[i]);
    }

    /*
      Compute the accumulator result portion of prevTupleDesc based on
      requested aggregate function invocations, and instantiate polymorphic
      AggComputers bound to correct inputs.
    */
    for (AggInvocationConstIter pInvocation(params.aggInvocations.begin());
         pInvocation != params.aggInvocations.end();
         ++pInvocation)
    {
        switch (pInvocation->aggFunction) {
        case AGG_FUNC_COUNT:
            prevTupleDesc.push_back(countDesc);
            break;
        case AGG_FUNC_SUM:
        case AGG_FUNC_MIN:
        case AGG_FUNC_MAX:
        case AGG_FUNC_SINGLE_VALUE:
            // Output type is same as input type, but nullable
            prevTupleDesc.push_back(inputDesc[pInvocation->iInputAttr]);
            prevTupleDesc.back().isNullable = true;
            break;
        }
        TupleAttributeDescriptor const *pInputAttr = NULL;
        if (pInvocation->iInputAttr != -1) {
            pInputAttr = &(inputDesc[pInvocation->iInputAttr]);
        }
        aggComputers.push_back(
            newAggComputer(
                pInvocation->aggFunction,
                pInputAttr));
        aggComputers.back().setInputAttrIndex(pInvocation->iInputAttr);
    }

    // Sanity check:  the output shape we computed should agree with
    // the descriptor (if any) in the supplied plan.
    if (!params.outputTupleDesc.empty()) {
        assert(prevTupleDesc == params.outputTupleDesc);
    }
    prevTuple.computeAndAllocate(prevTupleDesc);
    outputTuple.compute(prevTupleDesc);
    pOutAccessor->setTupleShape(prevTupleDesc);
}

AggComputer *SortedAggExecStream::newAggComputer(
    AggFunction aggFunction,
    TupleAttributeDescriptor const *pAttrDesc)
{
    return AggComputer::newAggComputer(aggFunction, pAttrDesc);
}

inline void SortedAggExecStream::clearAccumulator()
{
    for (int i = 0; i < aggComputers.size(); ++i) {
        aggComputers[i].clearAccumulator(prevTuple[i + groupByKeyCount]);
    }
}

inline void SortedAggExecStream::updateAccumulator()
{
    for (int i = 0; i < aggComputers.size(); ++i) {
        aggComputers[i].updateAccumulator(prevTuple[i + groupByKeyCount],
            inputTuple);
    }
}

inline void SortedAggExecStream::copyPrevGroupByKey()
{
    /*
      Need to make sure pointers are allocated before memcpy.
      resetBuffer restores the pointers to the associated buffer.
    */
    prevTuple.resetBuffer();

    for (int i = 0; i < groupByKeyCount; i ++) {
        prevTuple[i].memCopyFrom(inputTuple[i]);
    }
}

inline int SortedAggExecStream::compareGroupByKeys()
{
    /*
      prevTuple does not actually have the same Tuple layout
      as inputTuple; however, the prefixes(of length groupByKeyCount)
      refer to the same fields. Compare only the prefixes.
    */
    int ret =
        (pInAccessor->getTupleDesc()).compareTuplesKey(prevTuple,
                                                  inputTuple,
                                                  groupByKeyCount);
    return ret;
}

inline void SortedAggExecStream::computeOutput()
{
    int i;

    for (i = 0; i < groupByKeyCount; i ++) {
        outputTuple[i] = prevTuple[i];
    }

    for (i = 0; i < aggComputers.size(); i ++) {
        aggComputers[i].computeOutput(outputTuple[i + groupByKeyCount],
            prevTuple[i + groupByKeyCount]);
    }
}

void SortedAggExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    clearAccumulator();

    /*
      When accumulating, the first tuple in a group always updates the
      accumulator result field. Compare the group by key fields only for
      subsequent tuples. We use prevTupleValid to mark if the first tuple
      was seen.
      Need to set this in open() so that when the same stream is re-executed,
      e.g. when two identical group by statements are issued, the state when
      receiving the first input tuple of the first group is correct.
      Ignore prevTupleValid field when not doing groupby's.
    */
    prevTupleValid = (groupByKeyCount > 0) ? false : true;

    state = STATE_ACCUMULATING;
}

inline ExecStreamResult SortedAggExecStream::produce()
{
    assert (state == STATE_PRODUCING);

    // attempt to write output
    bool success = pOutAccessor->produceTuple(outputTuple);
    if (success) {
        clearAccumulator();
        state = STATE_ACCUMULATING;
        /*
          Succeeded in outputting result for the previous group.
          Record new group by key and update accumulator result fields.
        */
        copyPrevGroupByKey();
        updateAccumulator();
        pInAccessor->consumeTuple();
        return EXECRC_YIELD;
    } else {
        return EXECRC_BUF_OVERFLOW;
    }
}

ExecStreamResult SortedAggExecStream::execute(ExecStreamQuantum const &quantum)
{
    int keyComp;
    ExecStreamResult rc;

    /*
      Perform EOS processing first, since there can be a result tuple which is
      not produced yet.
    */
    if (pInAccessor->getState() == EXECBUF_EOS) {
        if (!prevTupleValid) {
            state = STATE_DONE;
        }

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
    } else if (state == STATE_PRODUCING) {
        rc = produce();
        if (rc != EXECRC_YIELD) {
            return rc;
        }
    }

    /*
      Check buffer state. If it is in a good state(EXECRC_YIELD, i.e. not in
      any abnormal state and is not empty),  process the tuples from the buffer.
    */
    rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    /*
      Iterate through all the INPUT tuples. In this method, quantum represents
      unit of input data.
    */
    for (uint nTuples = 0; nTuples < quantum.nTuplesMax; ++nTuples) {
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        assert (state == STATE_ACCUMULATING);

        pInAccessor->unmarshalTuple(inputTuple);

        if (prevTupleValid) {
            keyComp = compareGroupByKeys();
            assert(keyComp <= 0);
            if (keyComp == 0) {
                // continue reading rows and computing aggregates
                // for this group
                updateAccumulator();
                pInAccessor->consumeTuple();
            } else {
                // ready to produce an output row below
                computeOutput();
                state = STATE_PRODUCING;
            }
        } else {
            /*
              first tuple read so nothing to compare it to yet, but still need
              to record group by key and compute aggregates for that first row.
            */
            prevTupleValid = true;
            copyPrevGroupByKey();
            updateAccumulator();
            pInAccessor->consumeTuple();
        }

        if (state == STATE_PRODUCING) {
            rc = produce();
            if (rc != EXECRC_YIELD) {
                return rc;
            }
        }
    }

    return EXECRC_YIELD;
}

FENNEL_END_CPPFILE("$Id$");

// End SortedAggExecStream.cpp
