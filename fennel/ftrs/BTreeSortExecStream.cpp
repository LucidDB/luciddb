/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
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
#include "fennel/ftrs/BTreeSortExecStream.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeSortExecStream::prepare(BTreeSortExecStreamParams const &params)
{
    assert(params.rootPageId == NULL_PAGE_ID);
    assert(!params.pRootMap);
    assert(params.tupleDesc.empty());
    
    BTreeInsertExecStream::prepare(params);
}

// REVIEW:  do we ever want to save results on restart?
void BTreeSortExecStream::open(bool restart)
{
    sorted = false;
    BTreeBuilder builder(
        treeDescriptor,
        treeDescriptor.segmentAccessor.pSegment);
    builder.createEmptyRoot();
    treeDescriptor.rootPageId = builder.getRootPageId();

    // NOTE:  do this last so that rootPageId is available
    BTreeInsertExecStream::open(restart);
}

ExecStreamResult BTreeSortExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (!sorted) {
        if (pInAccessor->getState() == EXECBUF_EOS) {
            sorted = true;
        } else {
            return BTreeInsertExecStream::execute(quantum);
        }
    }
    
    if (!pWriter->isPositioned()) {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    uint nTuples = 0;
    TupleAccessor const &readAccessor = pWriter->getTupleAccessorForRead();
    
    do {
        uint cbBuffer = pOutAccessor->getProductionAvailable();
        PBuffer pBuffer = pOutAccessor->getProductionStart();
        uint cbTuple = readAccessor.getCurrentByteCount();
        if (cbBuffer < cbTuple) {
            if (nTuples > 0) {
                return EXECRC_OUTPUT;
            } else {
                return EXECRC_NEED_OUTPUTBUF;
            }
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
            return EXECRC_OUTPUT;
        }
    } while (pWriter->isPositioned());
    return EXECRC_OUTPUT;
}

void BTreeSortExecStream::closeImpl()
{
    BTreeInsertExecStream::closeImpl();
    BTreeBuilder builder(
        treeDescriptor,
        treeDescriptor.segmentAccessor.pSegment);
    bool rootless = true;
    builder.truncate(rootless);
    treeDescriptor.rootPageId = NULL_PAGE_ID;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeSortExecStream.cpp
