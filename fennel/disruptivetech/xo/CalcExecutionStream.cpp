/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2003-2004 Disruptive Tech
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
#include "fennel/disruptivetech/xo/CalcExecutionStream.h"
#include "fennel/common/ByteInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

CalcExecutionStream::~CalcExecutionStream()
{
}

void CalcExecutionStream::prepare(
    CalcExecutionStreamParams const &params,
    TupleDescriptor const &inputDesc,
    TupleDescriptor const &paramOutputDesc)
{
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
        << paramOutputDesc);

    assert(inputDesc.storageEqual(pCalc->getInputRegisterDescriptor()));

    outputDesc = pCalc->getOutputRegisterDescriptor();

    if (!paramOutputDesc.empty()) {
        assert(outputDesc.storageEqual(paramOutputDesc));

        // if the plan specifies an output descriptor with different
        // nullability, use that instead
        outputDesc = paramOutputDesc;
    }

    this->inputDesc = inputDesc;
    inputAccessor.compute(inputDesc);
    inputData.compute(inputDesc);

    outputAccessor.compute(outputDesc);
    outputData.compute(outputDesc);

    // bind calculator to tuple data (tuple data may later change)
    pCalc->bind(&inputData,&outputData);

    // Set calculator to return immediately on exception as a
    // workaround.  Prevents indeterminate results from an instruction
    // that throws an exception from causing non-deterministic
    // behavior later in program execution.
    pCalc->continueOnException(false);
}

TupleDescriptor const &CalcExecutionStream::getOutputDesc() const
{
    return outputDesc;
}

void CalcExecutionStream::closeImpl()
{
    inputAccessor.resetCurrentTupleBuf();
}

PBuffer CalcExecutionStream::execute(
    ByteInputStream &inputStream,
    PBuffer pNextOutputTuple,
    PBuffer pOutputBufferEnd)
{
    PConstBuffer pInputBufferEnd;
    PConstBuffer pInputBuffer = readNextBuffer(inputStream,pInputBufferEnd);
    if (!pInputBuffer) {
        return pNextOutputTuple;
    }
    PConstBuffer pNextInputTuple = pInputBuffer;
    PBuffer pFirstOutputTuple = pNextOutputTuple;

    for (;;) {
        while (!inputAccessor.getCurrentTupleBuf()) {
            if (pNextInputTuple >= pInputBufferEnd) {
                inputStream.consumeReadPointer(pNextInputTuple - pInputBuffer);
                pInputBuffer = readNextBuffer(inputStream,pInputBufferEnd);
                if (!pInputBuffer) {
                    return pNextOutputTuple;
                }
                pNextInputTuple = pInputBuffer;
            }
            inputAccessor.setCurrentTupleBuf(pNextInputTuple);
            inputAccessor.unmarshal(inputData);
            // FIXME: is any cleanup necessary?
            pCalc->exec();

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
                    pNextInputTuple += inputAccessor.getCurrentByteCount();
                    inputAccessor.resetCurrentTupleBuf();
                }
            }
        }
        if (!outputAccessor.isBufferSufficient(
                outputData,pOutputBufferEnd - pNextOutputTuple)) {
            // buffer should have fit at least one tuple
            assert (pNextOutputTuple > pFirstOutputTuple);
            inputStream.consumeReadPointer(pNextInputTuple - pInputBuffer);
            return pNextOutputTuple;
        }
        outputAccessor.marshal(outputData,pNextOutputTuple);
        pNextOutputTuple += outputAccessor.getCurrentByteCount();
        pNextInputTuple += inputAccessor.getCurrentByteCount();
        inputAccessor.resetCurrentTupleBuf();
    }
}

PConstBuffer CalcExecutionStream::readNextBuffer(
    ByteInputStream &inputStream,
    PConstBuffer &pInputBufferEnd)
{
    uint cbInputBuffer;
    PConstBuffer pInputBuffer = inputStream.getReadPointer(
        1,&cbInputBuffer);
    if (!pInputBuffer) {
        return NULL;
    }
    pInputBufferEnd = pInputBuffer + cbInputBuffer;
    return pInputBuffer;
}

FENNEL_END_CPPFILE("$Id$");

// End CalcExecutionStream.cpp

