/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
#include "fennel/disruptivetech/xo/CalcExcn.h"
#include "fennel/disruptivetech/xo/CalcExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void CalcExecStream::prepare(CalcExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    
    // Force instantiation of the calculator's instruction tables.
    (void) CalcInit::instance();

    pCalc.reset(new Calculator());
    if (isTracing()) {
        pCalc->initTraceSource(&(getTraceTarget()), "calc");
    }

    pCalc->assemble(params.program.c_str());

    if (params.isFilter) {
        pFilterDatum = &((*(pCalc->getStatusRegister()))[0]);
    } else {
        pFilterDatum = NULL;
    }

    FENNEL_TRACE(
        TRACE_FINER,
        "calc program = "
        << std::endl << params.program);

    FENNEL_TRACE(
        TRACE_FINER,
        "calc input TupleDescriptor = "
        << pCalc->getInputRegisterDescriptor());

    inputDesc = pInAccessor->getTupleDesc();
    FENNEL_TRACE(
        TRACE_FINER,
        "xo input TupleDescriptor = "
        << inputDesc);

    FENNEL_TRACE(
        TRACE_FINER,
        "calc output TupleDescriptor = "
        << pCalc->getOutputRegisterDescriptor());

    FENNEL_TRACE(
        TRACE_FINER,
        "xo output TupleDescriptor = "
        << params.outputTupleDesc);

    assert(inputDesc.storageEqual(pCalc->getInputRegisterDescriptor()));

    TupleDescriptor outputDesc = pCalc->getOutputRegisterDescriptor();

    if (!params.outputTupleDesc.empty()) {
        assert(outputDesc.storageEqual(params.outputTupleDesc));

        // if the plan specifies an output descriptor with different
        // nullability, use that instead
        outputDesc = params.outputTupleDesc;
    }
    pOutAccessor->setTupleShape(
        outputDesc,
        pInAccessor->getTupleFormat());

    inputData.compute(inputDesc);

    outputData.compute(outputDesc);

    // bind calculator to tuple data (tuple data may later change)
    pCalc->bind(&inputData,&outputData);

    // Set calculator to return immediately on exception as a
    // workaround.  Prevents indeterminate results from an instruction
    // that throws an exception from causing non-deterministic
    // behavior later in program execution.
    pCalc->continueOnException(false);
}

ExecStreamResult CalcExecStream::execute(ExecStreamQuantum const &quantum)
{
    switch (pInAccessor->getState()) {
    case EXECBUF_IDLE:
        pInAccessor->requestProduction();
        // NOTE:  fall through
    case EXECBUF_NEED_PRODUCTION:
        return EXECRC_NEED_INPUT;
    case EXECBUF_EOS:
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    case EXECBUF_NEED_CONSUMPTION:
        break;
    default:
        permAssert(false);
    }
    
    bool output = false;
    uint nTuplesProcessed = 0;

    for (;;) {
        while (!pInAccessor->isTupleUnmarshalled()) {
            if (pInAccessor->getState() != EXECBUF_NEED_CONSUMPTION) {
                if (output) {
                    return EXECRC_OUTPUT;
                } else {
                    return EXECRC_NEED_INPUT;
                }
            }
            if (nTuplesProcessed >= quantum.nTuplesMax) {
                if (output) {
                    return EXECRC_OUTPUT;
                } else {
                    return EXECRC_NO_OUTPUT;
                }
            }
            
            pInAccessor->unmarshalTuple(inputData);

            pCalc->exec();
            ++nTuplesProcessed;

            // REVIEW: JK 2004/7/16. Note that the calculator provides
            // two interfaces to the list of warnings. One is a
            // pre-parsed representation in the mWarnings deque --
            // this list may be easier for an upper level to digest --
            // instead of trying to pick apart the somewhat 'human
            // readable' serialized version in the warnings() string.
            if (pCalc->mWarnings.begin() != pCalc->mWarnings.end()) {
                throw CalcExcn(pCalc->warnings(), inputDesc, inputData);
            }
            
            if (pFilterDatum) {
                bool filterDiscard = *reinterpret_cast<bool const *>(
                    pFilterDatum->pData);
                if (filterDiscard) {
                    pInAccessor->consumeUnmarshalledTuple();
                }
            }
        }
        
        if (pOutAccessor->produceTuple(outputData)) {
            output = true;
        } else {
            if (output) {
                return EXECRC_OUTPUT;
            } else {
                return EXECRC_NEED_OUTPUTBUF;
            }
        }

        pInAccessor->consumeUnmarshalledTuple();
    }
}

FENNEL_END_CPPFILE("$Id$");

// End CalcExecStream.cpp
