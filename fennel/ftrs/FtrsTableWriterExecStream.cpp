/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
}

void FtrsTableWriterExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity,optQuantity);
    
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
    assert(pGraph->getTxn());
    // REVIEW:  close/restart?
    pGraph->getTxn()->addParticipant(pTableWriter);
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
        // REVIEW: The format for a 1-tuple with a single non-null uint64_t
        // value is just the value itself (assuming alignment size no greater
        // than 64-bit), which is why this works.  But it would be cleaner to
        // set up a proper TupleAccessor.
        pOutAccessor->provideBufferForConsumption(
            reinterpret_cast<PConstBuffer>(&nTuples), 
            reinterpret_cast<PConstBuffer>((&nTuples) + 1));
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
    svptId = pGraph->getTxn()->createSavepoint();
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
    pGraph->getTxn()->commitSavepoint(svptIdCopy);
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
    pGraph->getTxn()->rollback(&svptIdCopy);
    pGraph->getTxn()->commitSavepoint(svptIdCopy);
}

FENNEL_END_CPPFILE("$Id$");

// End FtrsTableWriterExecStream.cpp
