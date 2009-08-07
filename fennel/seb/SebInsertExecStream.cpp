/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
#include "fennel/seb/SebInsertExecStream.h"
#include "fennel/seb/SebCmdInterpreter.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

#include "scaledb/incl/SdbStorageAPI.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SebInsertExecStream::SebInsertExecStream()
{
}

void SebInsertExecStream::prepare(SebInsertExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    tableId = params.tableId;
    inputTuple.compute(pInAccessor->getTupleDesc());
    TupleAccessor &outputTupleAccessor =
        pOutAccessor->getScratchTupleAccessor();
    TupleDescriptor const &outputTupleDesc = pOutAccessor->getTupleDesc();
    outputTuple.compute(outputTupleDesc);
    outputTuple[0].pData = reinterpret_cast<PBuffer>(&nTuples);
    outputTupleBuffer.reset(
        new FixedBuffer[outputTupleAccessor.getMaxByteCount()]);
}

void SebInsertExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    nTuples = 0;
    isDone = false;
}

ExecStreamResult SebInsertExecStream::execute(ExecStreamQuantum const &quantum)
{
    if (isDone) {
        // already returned final result
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    if (pInAccessor->getState() == EXECBUF_EOS) {
        // we've processed all input,  so return row count as our output
        TupleAccessor &outputTupleAccessor =
            pOutAccessor->getScratchTupleAccessor();
        outputTupleAccessor.marshal(outputTuple, outputTupleBuffer.get());
        pOutAccessor->provideBufferForConsumption(
            outputTupleBuffer.get(),
            outputTupleBuffer.get()
            + outputTupleAccessor.getCurrentByteCount());
        isDone = true;
        return EXECRC_BUF_OVERFLOW;
    }

    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    do {
        if (!pInAccessor->demandData()) {
            break;
        }
        TupleDescriptor const &inputDesc = pInAccessor->getTupleDesc();
        pInAccessor->unmarshalTuple(inputTuple);
        for (uint i = 0; i < inputTuple.size(); ++i) {
            TupleAttributeDescriptor const &attrDesc = inputDesc[i];
            TupleDatum const &datum = inputTuple[i];
            StandardTypeDescriptorOrdinal typeOrdinal =
                static_cast<StandardTypeDescriptorOrdinal>(
                    attrDesc.pTypeDescriptor->getOrdinal());
            // TODO jvs 12-Jul-2009:  null values
            if (StandardTypeDescriptor::isArray(typeOrdinal)) {
                SDBPrepareStrField(
                    SebCmdInterpreter::getUserId(),
                    SebCmdInterpreter::getDbId(),
                    tableId,
                    i + 1,
                    (char *) datum.pData,
                    datum.cbData);
            } else {
                SDBPrepareNumberField(
                    SebCmdInterpreter::getUserId(),
                    SebCmdInterpreter::getDbId(),
                    tableId,
                    i + 1,
                    (char *) datum.pData);
            }
        }
        // TODO jvs 12-Jul-2009:  excn check for retVal here and above
        SDBInsertRow(
            SebCmdInterpreter::getUserId(),
            SebCmdInterpreter::getDbId(),
            tableId,
            NULL,
            0,
            0);
        pInAccessor->consumeTuple();
        ++nTuples;
    } while (nTuples < quantum.nTuplesMax);

    if (!pInAccessor->isConsumptionPossible()) {
        return EXECRC_BUF_UNDERFLOW;
    } else {
        return EXECRC_QUANTUM_EXPIRED;
    }
}

void SebInsertExecStream::closeImpl()
{
    ConduitExecStream::closeImpl();
}

ExecStreamBufProvision SebInsertExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End SebInsertExecStream.cpp
