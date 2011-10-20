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
#include "fennel/exec/MockConsumerExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");

MockConsumerExecStreamTupleChecker::~MockConsumerExecStreamTupleChecker()
{
}

ExecStreamResult MockConsumerExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    ExecStreamBufAccessor &inAccessor = *pInAccessor;
    switch (inAccessor.getState()) {
    case EXECBUF_EMPTY:
        inAccessor.requestProduction();
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_UNDERFLOW:
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_EOS:
        recvEOS = true;
        return EXECRC_EOS;
    case EXECBUF_NONEMPTY:
    case EXECBUF_OVERFLOW:
        break;
    default:
        permFail("Bad state " << inAccessor.getState());
    }
    assert(inAccessor.isConsumptionPossible());

    uint64_t oldRowCount = rowCount;
    uint64_t oldIncorrectRowCount = incorrectRowCount;
    ExecStreamResult rc = innerExecute(quantum);
    FENNEL_TRACE(
        TRACE_FINE, "read " << (rowCount - oldRowCount)
        << " rows, rowCount is " << rowCount);
    if (checkData) {
        long n = incorrectRowCount - oldIncorrectRowCount;
        FENNEL_TRACE(TRACE_FINE, "TupleChecker rejected " << n << " rows");
    }
    if (maxConsecutiveUnderflows > 0) {
        if (rc == EXECRC_BUF_UNDERFLOW) {
            if (++consecutiveUnderflowCt > maxConsecutiveUnderflows) {
                FENNEL_TRACE(
                    TRACE_CONFIG, "over consecutive underflow max "
                    << maxConsecutiveUnderflows);
                rc = EXECRC_EOS;
            }
        } else {
            consecutiveUnderflowCt = 0;
        }
    }
    return rc;
}

ExecStreamResult MockConsumerExecStream::innerExecute(
    ExecStreamQuantum const& quantum)
{
    ExecStreamBufAccessor &inAccessor = *pInAccessor;
    const TupleDescriptor& inputDesc = inAccessor.getTupleDesc();

    // Read rows from the input buffer until we exceed the quantum or read all
    // of the rows.
    for (uint iRow = 0; iRow < quantum.nTuplesMax; ++iRow) {
        if (dieOnNthRow > 0) {
            if (rowCount + 1 > dieOnNthRow) {
                throw FennelExcn("fake fatal");
            }
        }
        if (!inAccessor.demandData()) {
            // Convert buf return code into stream return code.
            switch (inAccessor.getState()) {
            case EXECBUF_UNDERFLOW:
                return EXECRC_BUF_UNDERFLOW;
            case EXECBUF_EOS:
                recvEOS = true;
                return EXECRC_EOS;
            default:
                permAssert(false);
            }
        }
        inAccessor.unmarshalTuple(inputTuple);
        if (checkData) {
            if (!checkData->check(rowCount, inputDesc, inputTuple)) {
                incorrectRowCount++;
            }
        }
        if (echoData) {
            tuplePrinter.print(*echoData, inputDesc, inputTuple);
        }
        if (saveData) {
            // Convert each row to a string, and append to the rows vector.
            std::ostringstream oss;
            tuplePrinter.print(oss, inputDesc, inputTuple);
            const string &s = oss.str();
            rowStrings.push_back(s);
        }
        inAccessor.consumeTuple();
        rowCount++;
    }
    return EXECRC_QUANTUM_EXPIRED;
}

MockConsumerExecStream::~MockConsumerExecStream()
{
}

MockConsumerExecStream::MockConsumerExecStream()
{
    saveData = recvEOS = false;
    echoData = 0;
    checkData = 0;
    dieOnNthRow = rowCount = incorrectRowCount = 0;
}

void MockConsumerExecStream::prepare(
    MockConsumerExecStreamParams const &params)
{
    SingleInputExecStream::prepare(params);
    saveData = params.saveData;
    echoData = params.echoData;
    checkData = params.checkData;
    maxConsecutiveUnderflows = params.maxConsecutiveUnderflows;
    dieOnNthRow = params.dieOnNthRow;
    recvEOS = false;
}

void MockConsumerExecStream::open(bool restart)
{
    SingleInputExecStream::open(restart);
    rowCount = incorrectRowCount = 0;
    consecutiveUnderflowCt = 0;
    rowStrings.clear();
    inputTuple.compute(pInAccessor->getTupleDesc());
    recvEOS = false;
}


FENNEL_END_CPPFILE("$Id$");

// End MockConsumerExecStream.cpp
