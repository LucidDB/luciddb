/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/xo/CalcExecutionStream.h"
#include "fennel/common/ByteInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

CalcExecutionStream::~CalcExecutionStream()
{
}

void CalcExecutionStream::prepare(
    CalcExecutionStreamParams const &params,
    TupleDescriptor const &inputDesc)
{
    pCalc.reset(new Calculator());
    pCalc->assemble(params.program.c_str());

    if (params.isFilter) {
        pFilterDatum = &((*(pCalc->getStatusRegister()))[0]);
    } else {
        pFilterDatum = NULL;
    }
    
    assert(pCalc->getInputRegisterDescriptor() == inputDesc);

    inputAccessor.compute(inputDesc);
    inputData.compute(inputDesc);

    outputDesc = pCalc->getOutputRegisterDescriptor();

    outputAccessor.compute(outputDesc);
    outputData.compute(outputDesc);

    // bind calculator to tuple data (tuple data may later change)
    pCalc->bind(&inputData,&outputData);
}

TupleDescriptor const &CalcExecutionStream::getOutputDesc() const
{
    return outputDesc;
}

void CalcExecutionStream::closeImpl()
{
    pCalc.reset();
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
        return false;
    }
    PConstBuffer pNextInputTuple = pInputBuffer;

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
            pCalc->exec();
            if (pFilterDatum) {
                bool filterPassed = *reinterpret_cast<bool const *>(
                    pFilterDatum->pData);
                if (!filterPassed) {
                    pNextInputTuple += inputAccessor.getCurrentByteCount();
                    inputAccessor.resetCurrentTupleBuf();
                }
            }
        }
        if (!outputAccessor.isBufferSufficient(
                outputData,pOutputBufferEnd - pNextOutputTuple)) {
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
