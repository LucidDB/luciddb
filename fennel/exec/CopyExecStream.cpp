/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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
#include "fennel/exec/CopyExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void CopyExecStream::prepare(CopyExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
}

ExecStreamResult CopyExecStream::execute(ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    uint cbAvailableIn = pInAccessor->getConsumptionAvailable();
    uint cbAvailableOut = pOutAccessor->getProductionAvailable();

    PConstBuffer pSrc = pInAccessor->getConsumptionStart();
    PBuffer pDst = pOutAccessor->getProductionStart();

    if (cbAvailableOut < cbAvailableIn) {
        // In a non-DFS scheduler, the first call to this XO may be made before
        // the first call to its consumer. If so, the consumer may not have
        // allocated its buffer; punt until it has.
        if (cbAvailableOut == 0 && pOutAccessor->getState() == EXECBUF_EMPTY) {
            return EXECRC_BUF_OVERFLOW;
        }
        // oops, impedance mismatch:  have to figure out how many
        // complete tuples we can safely copy without overflow
        cbAvailableIn =
            pInAccessor->getConsumptionAvailableBounded(cbAvailableOut);
        permAssert(cbAvailableIn > 0);
    } else {
        rc = EXECRC_BUF_UNDERFLOW;
    }

    memcpy(pDst, pSrc, cbAvailableIn);
    pInAccessor->consumeData(pSrc + cbAvailableIn);
    pOutAccessor->produceData(pDst + cbAvailableIn);

    // we can't use whatever's left in output buffer, so tell consumer
    // to give us a fresh one next time
    pOutAccessor->requestConsumption();

    if ((rc == EXECRC_BUF_UNDERFLOW)
        && (pInAccessor->getState() != EXECBUF_EOS))
    {
        pInAccessor->requestProduction();
    }

    return EXECRC_BUF_OVERFLOW;
}

// TODO jvs 20-Nov-2006:  move this to ExecStreamBufAccessor.cpp
// once it exists
uint ExecStreamBufAccessor::getConsumptionAvailableBounded(uint cbLimit)
{
    uint cbAvailable = getConsumptionAvailable();
    if (cbAvailable <= cbLimit) {
        return cbAvailable;
    }

    TupleAccessor const &tupleAccessor = getConsumptionTupleAccessor();
    PConstBuffer pSrc = getConsumptionStart();

    PConstBuffer pTuple = pSrc;
    PConstBuffer pTupleSafe = pTuple;
    PConstBuffer pEnd = pSrc + cbLimit;
    for (;;) {
        uint cbTuple = tupleAccessor.getBufferByteCount(pTuple);
        pTuple += cbTuple;
        if (pTuple > pEnd) {
            // this tuple would put us over the limit
            break;
        }
        // this tuple will fit
        pTupleSafe = pTuple;
    }
    return pTupleSafe - pSrc;
}

FENNEL_END_CPPFILE("$Id$");

// End CopyExecStream.cpp
