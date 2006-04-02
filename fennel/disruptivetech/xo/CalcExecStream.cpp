/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 1999-2005 John V. Sichi
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
#include "fennel/disruptivetech/xo/CalcExcn.h"
#include "fennel/disruptivetech/xo/CalcExecStream.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void CalcExecStream::prepare(CalcExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    try {
        // Force instantiation of the calculator's instruction tables.
        (void) CalcInit::instance();

        pCalc.reset(new Calculator(pDynamicParamManager.get()));
        if (isTracing()) {
            pCalc->initTraceSource(getSharedTraceTarget(), "calc");
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

    } catch (FennelExcn e) {
        FENNEL_TRACE(TRACE_SEVERE, "error preparing calculator: " << e.getMessage());
        throw e;
    }
}

void CalcExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);

    // Zero out status registers.
    if (pCalc != NULL) {
        pCalc->zeroStatusRegister();
    }
}

ExecStreamResult CalcExecStream::execute(ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

#define TRACE_RETURN FENNEL_TRACE(TRACE_FINE, "read " << nRead << " rows, wrote " << nWritten)

    try {
        FENNEL_TRACE(TRACE_FINER, "start execute loop");
        uint nRead = 0;
        uint nWritten = 0;
        while (nRead < quantum.nTuplesMax) {
            while (!pInAccessor->isTupleConsumptionPending()) {
                if (!pInAccessor->demandData()) {
                    TRACE_RETURN;
                    return EXECRC_BUF_UNDERFLOW;
                }

                FENNEL_TRACE(TRACE_FINER, "input row " << nRead);
                pInAccessor->unmarshalTuple(inputData);
                pCalc->exec();
                bool skip = false;
                if (! pCalc->mWarnings.empty()) {
                    // calculator failed to produce a row
                    // REVIEW: Do we need to distinguish errors from warnings here?
                    // TODO: notify scheduler (interface TBD)
                    //  which can warn user or produce other side effects.
                    FENNEL_TRACE(TRACE_WARNING, "calculator error " << pCalc->warnings());
                    skip = true;
                } else if (pFilterDatum) {
                    bool filterDiscard =
                        *reinterpret_cast<bool const *>(pFilterDatum->pData);
                    if (filterDiscard) {
                        skip = true;
                    }
                }
                if (skip) {
                    FENNEL_TRACE(TRACE_FINER, "skip row " << nRead);
                    pInAccessor->consumeTuple();
                    ++nRead;
                }
            }
        
            FENNEL_TRACE(TRACE_FINER, "output row " << nWritten);
            if (!pOutAccessor->produceTuple(outputData)) {
                TRACE_RETURN;
                return EXECRC_BUF_OVERFLOW;
            }
            ++nWritten;
            pInAccessor->consumeTuple();
            ++nRead;
        }
        TRACE_RETURN;
        return EXECRC_QUANTUM_EXPIRED;

    } catch (FennelExcn e) {
        FENNEL_TRACE(TRACE_SEVERE, "error executing calculator: " << e.getMessage());
        throw e;
    }
#undef TRACE_RETURN
}

FENNEL_END_CPPFILE("$Id$");

// End CalcExecStream.cpp
