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
#include "fennel/seb/SebQueryExecStream.h"
#include "fennel/seb/SebCmdInterpreter.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

#include "scaledb/incl/SdbStorageAPI.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SebQueryExecStream::SebQueryExecStream()
{
    queryMgrId = MAXU;
}

void SebQueryExecStream::prepare(SebQueryExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);
    tupleData.compute(pOutAccessor->getTupleDesc());
    tableId = params.tableId;
    indexId = params.indexId;
    outputProj = params.outputProj;
}

void SebQueryExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
    // REVIEW jvs 14-Jul-2009:  binding between query mgrs, threads, etc
    queryMgrId = SDBGetQueryManagerId(SebCmdInterpreter::getUserId());
    // TODO jvs 13-Jul-2009:  check retVals
    SDBResetQuery(queryMgrId);
    SDBDefineQuery(
        queryMgrId,
        SebCmdInterpreter::getDbId(),
        indexId,
        (char *) "field0",
        NULL);
    SDBPrepareQuery(
        queryMgrId,
        0,
        0,
        true);
    tuplePending = false;
    done = false;
}

ExecStreamResult SebQueryExecStream::execute(ExecStreamQuantum const &quantum)
{
    uint nTuples = 0;

    for (;;) {
        if (tuplePending) {
            if (pOutAccessor->produceTuple(tupleData)) {
                tuplePending = false;
                ++nTuples;
            } else {
                return EXECRC_BUF_OVERFLOW;
            }
        }
        if (done) {
            break;
        }
        if (SDBNext(queryMgrId)) {
            done = true;
            break;
        }
        // TODO jvs 14-Jul-2009:  deal with nulls, variable length data, etc.
        for (uint i = 0; i < tupleData.size(); ++i) {
            uint iField = outputProj[i] + 1;
            unsigned int cbData;
            char *pData = SDBQueryCursorGetFieldByTableDataSize(
                queryMgrId,
                tableId,
                iField,
                &cbData);
            tupleData[i].pData = reinterpret_cast<PBuffer>(pData);
            tupleData[i].cbData = cbData;
        }
        tuplePending = true;
        if (nTuples >= quantum.nTuplesMax) {
            return EXECRC_QUANTUM_EXPIRED;
        }
    }

    if (!nTuples) {
        pOutAccessor->markEOS();
    }
    return EXECRC_EOS;
}

void SebQueryExecStream::closeImpl()
{
    SingleOutputExecStream::closeImpl();
    if (!isMAXU(queryMgrId)) {
        SDBCloseQueryManager(queryMgrId);
        queryMgrId = MAXU;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End SebQueryExecStream.cpp
