/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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
#include "fennel/exec/BarrierExecStream.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");
void BarrierExecStream:: prepare(BarrierExecStreamParams const &params)
{
    TupleDescriptor outputTupleDesc;

    ConfluenceExecStream::prepare(params);
    returnMode = params.returnMode;
    parameterIds = params.parameterIds;
    outputTupleDesc = inAccessors[0]->getTupleDesc();

    // validate the input and output descriptors
    assert(outputTupleDesc == pOutAccessor->getTupleDesc());
    if (parameterIds.size() > 0) {
        assert(outputTupleDesc.size() == 1);
        dynParamVal.compute(outputTupleDesc);
    }
    if (!returnFirstInput()) {
        for (uint i = 1; i < inAccessors.size(); i ++) {
            assert(outputTupleDesc == inAccessors[i]->getTupleDesc());
        }
    }

    if (returnAnyInput()) {
        inputTuple.compute(outputTupleDesc);
        compareTuple.compute(outputTupleDesc);
    }

    outputTupleAccessor = &pOutAccessor->getScratchTupleAccessor();
    outputBufSize = outputTupleAccessor->getMaxByteCount();
    uint nRows = parameterIds.size();
    if (returnAllInputs()) {
        nRows += inAccessors.size();
    } else {
        nRows += 1;
    }
    outputBufSize *= nRows;
}

void BarrierExecStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);
    iInput = 0;

    if (!restart) {
        outputTupleBuffer.reset(new FixedBuffer[outputBufSize]);
    }
    curOutputPos = 0;
    isDone = false;
}

ExecStreamResult BarrierExecStream::execute(ExecStreamQuantum const &quantum)
{
    if (isDone) {
        // already returned final result
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    switch (pOutAccessor->getState()) {
    case EXECBUF_NONEMPTY:
    case EXECBUF_OVERFLOW:
        return EXECRC_BUF_OVERFLOW;
    case EXECBUF_UNDERFLOW:
    case EXECBUF_EMPTY:
        break;
    case EXECBUF_EOS:
        return EXECRC_EOS;
    }

    while (iInput < inAccessors.size()) {
        switch (inAccessors[iInput]->getState()) {
        case EXECBUF_OVERFLOW:
        case EXECBUF_NONEMPTY:
            processInputTuple();
            // fall through
        case EXECBUF_UNDERFLOW:
            return EXECRC_BUF_UNDERFLOW;
        case EXECBUF_EMPTY:
            inAccessors[iInput]->requestProduction();
            return EXECRC_BUF_UNDERFLOW;
        case EXECBUF_EOS:
            ++iInput;
            break;
        default:
            permAssert(false);
        }
    }

    // write out the data passed in via dynamic parameters
    for (uint i = 0; i < parameterIds.size(); i++) {
        DynamicParam const &param =
            pDynamicParamManager->getParam(parameterIds[i]);
        assert(param.getDesc() == inAccessors[0]->getTupleDesc()[0]);
        dynParamVal[0] = param.getDatum();
        outputTupleAccessor->marshal(
            dynParamVal,
            outputTupleBuffer.get() + curOutputPos);
        curOutputPos += outputTupleAccessor->getCurrentByteCount();
    }

    // Write out the output buffer and indicate OVERFLOW.
    pOutAccessor->provideBufferForConsumption(
        outputTupleBuffer.get(),
        outputTupleBuffer.get() + outputBufSize);

    // close the producers to free up resources
    ExecStreamGraphImpl &graphImpl =
        dynamic_cast<ExecStreamGraphImpl&>(getGraph());
    graphImpl.closeProducers(getStreamId());
    isDone = true;

    return EXECRC_BUF_OVERFLOW;
}

ExecStreamBufProvision
    BarrierExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

void BarrierExecStream::closeImpl()
{
    ConfluenceExecStream::closeImpl();
    outputTupleBuffer.reset();
}

void BarrierExecStream::processInputTuple()
{
    switch (returnMode) {
    case BARRIER_RET_FIRST_INPUT:
    case BARRIER_RET_ANY_INPUT:
        // copy input to output if first input
        if (iInput == 0) {
            curOutputPos +=
                copyInputData(outputTupleBuffer.get(), inAccessors[iInput]);
            outputTupleAccessor->setCurrentTupleBuf(outputTupleBuffer.get());
            outputTupleAccessor->unmarshal(compareTuple);
        } else if (returnAnyInput()) {
            // sanity check in the case where all inputs are supposed to
            // return the same output -- make sure that is the case
            inAccessors[iInput]->unmarshalTuple(inputTuple);
            permAssert(
                (inAccessors[iInput]->getTupleDesc()).compareTuples(
                    inputTuple, compareTuple) == 0);
        } else {
            inAccessors[iInput]->accessConsumptionTuple();
        }
        break;

    case BARRIER_RET_ALL_INPUTS:
        // copy the entire input tuple to the apppropriate position in
        // the output buffer
        curOutputPos +=
            copyInputData(
                outputTupleBuffer.get() + curOutputPos,
                inAccessors[iInput]);
        break;

    default:
        permAssert(false);
    }

    inAccessors[iInput]->consumeTuple();
}

uint BarrierExecStream::copyInputData(
    PBuffer destBuffer,
    SharedExecStreamBufAccessor &pInAccessor)
{
    uint nBytes = pInAccessor->accessConsumptionTuple().getCurrentByteCount();
    memcpy(
        destBuffer,
        pInAccessor->getConsumptionStart(),
        nBytes);
    return nBytes;
}

FENNEL_END_CPPFILE("$Id$");

// End BarrierExecStream.cpp
