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
#include "fennel/lcs/LcsCountAggExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LcsCountAggExecStream::LcsCountAggExecStream()
{
}

ExecStreamBufProvision LcsCountAggExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

void LcsCountAggExecStream::prepare(LcsRowScanExecStreamParams const &params)
{
    setCountAgg();
    LcsRowScanExecStream::prepare(params);
    // Only LCS_RID should be projected.
    permAssert(params.outputProj.size() == 1);
    permAssert(params.outputProj[0] == LCS_RID_COLUMN_ID);
    permAssert(pOutAccessor->getTupleDesc().size() == 1);
    TupleAttributeDescriptor const &attrDesc =
        pOutAccessor->getTupleDesc()[0];
    permAssert(!attrDesc.isNullable);
    permAssert(attrDesc.pTypeDescriptor->getOrdinal() == STANDARD_TYPE_INT_64);
    pOutputTupleAccessor = &(pOutAccessor->getScratchTupleAccessor());
    outputTupleBuffer.reset(
        new FixedBuffer[pOutputTupleAccessor->getMaxByteCount()]);
}

ExecStreamResult LcsCountAggExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (pOutAccessor->hasPendingEOS()
        || pOutAccessor->getState() == EXECBUF_EOS)
    {
        return EXECRC_EOS;
    }
    ExecStreamResult rc = LcsRowScanExecStream::execute(quantum);
    if (rc != EXECRC_EOS) {
        return rc;
    }
    // Write out final row count.
    pOutAccessor->clear();
    RecordNum nRows = getRowCount();
    getProjOutputTupleData()[0].pData = reinterpret_cast<PConstBuffer>(&nRows);
    pOutputTupleAccessor->marshal(
        getProjOutputTupleData(), outputTupleBuffer.get());
    pOutAccessor->provideBufferForConsumption(
        outputTupleBuffer.get(),
        outputTupleBuffer.get()
        + pOutputTupleAccessor->getCurrentByteCount());
    pOutAccessor->markEOS();
    return EXECRC_BUF_OVERFLOW;
}


FENNEL_END_CPPFILE("$Id$");

// End LcsCountAggExecStream.cpp
