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
#include "fennel/ftrs/FtrsTableWriterExecStream.h"
#include "fennel/ftrs/FtrsTableWriterFactory.h"
#include "fennel/txn/LogicalTxn.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");

FtrsTableWriterExecStream::FtrsTableWriterExecStream()
{
    pActionMutex = NULL;
    svptId = NULL_SVPT_ID;
}

void FtrsTableWriterExecStream::prepare(
    FtrsTableWriterExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    pTableWriter = params.pTableWriterFactory->newTableWriter(params);
    actionType = params.actionType;
    pActionMutex = params.pActionMutex;
    assert(pActionMutex);

    TupleAccessor &outputTupleAccessor =
        pOutAccessor->getScratchTupleAccessor();
    TupleDescriptor const &outputTupleDesc = pOutAccessor->getTupleDesc();
    outputTuple.compute(outputTupleDesc);
    outputTuple[0].pData = reinterpret_cast<PBuffer>(&nTuples);
    outputTupleBuffer.reset(
        new FixedBuffer[outputTupleAccessor.getMaxByteCount()]);
}

void FtrsTableWriterExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity, optQuantity);

    // REVIEW:  update/delete resources

    // This is to account for total number of pages needed to perform an
    // update on a single index.  Pages are only locked for the duration of
    // one index update, so they don't need to be charged per index (unless
    // we start parallelizing index updates).  REVIEW: determine the correct
    // number; 4 is just a guess.
    minQuantity.nCachePages += 4;

    // each BTreeWriter currently needs a private scratch page
    minQuantity.nCachePages += pTableWriter->getIndexCount();

    optQuantity = minQuantity;
}

void FtrsTableWriterExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    assert(pTxn);

    // REVIEW:  close/restart?

    // block checkpoints while joining txn
    SXMutexSharedGuard actionMutexGuard(*pActionMutex);
    pTxn->addParticipant(pTableWriter);
    actionMutexGuard.unlock();

    nTuples = 0;
    pTableWriter->openIndexWriters();
    isDone = false;
}

ExecStreamResult FtrsTableWriterExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (isDone) {
        // already returned final result
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    if (pInAccessor->getState() == EXECBUF_EOS) {
        // we've processed all input,  so commit what we've written
        // and return row count as our output
        commitSavepoint();

        // TODO jvs 11-Feb-2006:  Other streams (e.g.
        // LcsClusterAppendExecStream) need to do something similar,
        // so provide some utilities to make it easier.
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

    if (svptId == NULL_SVPT_ID) {
        createSavepoint();
    }

    try {
        nTuples += pTableWriter->execute(
            quantum, *pInAccessor, actionType, *pActionMutex);
    } catch (...) {
        try {
            rollbackSavepoint();
        } catch (...) {
            // TODO:  trace failed rollback
        }
        throw;
    }

    if (!pInAccessor->isConsumptionPossible()) {
        return EXECRC_BUF_UNDERFLOW;
    } else {
        return EXECRC_QUANTUM_EXPIRED;
    }
}

ExecStreamBufProvision
    FtrsTableWriterExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

void FtrsTableWriterExecStream::closeImpl()
{
    if (svptId != NULL_SVPT_ID) {
        rollbackSavepoint();
    }
    ConduitExecStream::closeImpl();
    if (pTableWriter) {
        pTableWriter->closeIndexWriters();
    }
}

void FtrsTableWriterExecStream::createSavepoint()
{
    // block checkpoints while creating savepoint
    SXMutexSharedGuard actionMutexGuard(*pActionMutex);
    svptId = pTxn->createSavepoint();
}

void FtrsTableWriterExecStream::commitSavepoint()
{
    if (svptId == NULL_SVPT_ID) {
        return;
    }

    SavepointId svptIdCopy = svptId;
    svptId = NULL_SVPT_ID;

    // block checkpoints while committing savepoint
    SXMutexSharedGuard actionMutexGuard(*pActionMutex);
    pTxn->commitSavepoint(svptIdCopy);
}

void FtrsTableWriterExecStream::rollbackSavepoint()
{
    if (svptId == NULL_SVPT_ID) {
        return;
    }

    SavepointId svptIdCopy = svptId;
    svptId = NULL_SVPT_ID;

    // block checkpoints while rolling back savepoint
    SXMutexSharedGuard actionMutexGuard(*pActionMutex);
    pTxn->rollback(&svptIdCopy);
    pTxn->commitSavepoint(svptIdCopy);
}

FENNEL_END_CPPFILE("$Id$");

// End FtrsTableWriterExecStream.cpp
