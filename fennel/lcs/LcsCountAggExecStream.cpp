/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 Dynamo BI Corporation
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
