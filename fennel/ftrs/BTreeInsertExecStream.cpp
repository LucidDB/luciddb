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
#include "fennel/ftrs/BTreeInsertExecStream.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeInsertExecStream::prepare(BTreeInsertExecStreamParams const &params)
{
    BTreeExecStream::prepare(params);
    ConduitExecStream::prepare(params);
    distinctness = params.distinctness;

    assert(treeDescriptor.tupleDescriptor == pInAccessor->getTupleDesc());
}

void BTreeInsertExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    BTreeExecStream::getResourceRequirements(minQuantity,optQuantity);
    
    // max number of pages locked during tree update (REVIEW),
    // including BTreeWriter's private scratch page
    minQuantity.nCachePages += 5;
    
    // TODO:  use opt to govern prefetch and come up with a good formula
    optQuantity = minQuantity;
}

void BTreeInsertExecStream::open(bool restart)
{
    BTreeExecStream::open(restart);
    ConduitExecStream::open(restart);
    
    if (restart) {
        return;
    }
    
    pWriter = newWriter();
}

ExecStreamResult BTreeInsertExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitInput();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    uint nTuples = 0;
    
    for (;;) {
        PConstBuffer pTupleBuf = pInAccessor->getConsumptionStart();
        uint cb = pWriter->insertTupleFromBuffer(pTupleBuf,distinctness);
        pInAccessor->consumeData(pTupleBuf + cb);
        ++nTuples;
        if (nTuples > quantum.nTuplesMax) {
            return EXECRC_QUANTUM_EXPIRED;
        }
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }
    }
}

void BTreeInsertExecStream::closeImpl()
{
    BTreeExecStream::closeImpl();
    pWriter.reset();
    pBTreeAccessBase.reset();
    ConduitExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeInsertExecStream.cpp
