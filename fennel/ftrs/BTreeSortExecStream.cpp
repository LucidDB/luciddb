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
#include "fennel/ftrs/BTreeSortExecStream.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeSortExecStream::prepare(BTreeSortExecStreamParams const &params)
{
    assert(params.rootPageId == NULL_PAGE_ID);
    assert(!params.pRootMap);

    BTreeInsertExecStream::prepare(params);
    dynamicBTree = true;
    truncateOnRestart = true;
}

// REVIEW:  do we ever want to save results on restart?
void BTreeSortExecStream::open(bool restart)
{
    sorted = false;
    BTreeInsertExecStream::open(restart);
}

ExecStreamResult BTreeSortExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (!sorted) {
        if (pInAccessor->getState() == EXECBUF_EOS) {
            sorted = true;
            bool found = pWriter->searchFirst();
            if (!found) {
                pWriter->endSearch();
            }
        } else {
            return BTreeInsertExecStream::execute(quantum);
        }
    }

    if (!pWriter->isPositioned()) {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    if (pOutAccessor->getState() == EXECBUF_OVERFLOW) {
        return EXECRC_BUF_OVERFLOW;
    }

    uint nTuples = 0;
    TupleAccessor const &readAccessor = pWriter->getTupleAccessorForRead();

    do {
        uint cbBuffer = pOutAccessor->getProductionAvailable();
        PBuffer pBuffer = pOutAccessor->getProductionStart();
        uint cbTuple = readAccessor.getCurrentByteCount();
        if (cbBuffer < cbTuple) {
            pOutAccessor->requestConsumption();
            return EXECRC_BUF_OVERFLOW;
        }
        memcpy(
            pBuffer,
            readAccessor.getCurrentTupleBuf(),
            cbTuple);
        pOutAccessor->produceData(pBuffer + cbTuple);
        ++nTuples;
        if (!pWriter->searchNext()) {
            pWriter->endSearch();
        }
        if (nTuples >= quantum.nTuplesMax) {
            return EXECRC_QUANTUM_EXPIRED;
        }
    } while (pWriter->isPositioned());
    return EXECRC_EOS;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeSortExecStream.cpp
