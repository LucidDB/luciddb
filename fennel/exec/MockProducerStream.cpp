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
#include "fennel/exec/MockProducerStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void MockProducerStream::prepare(MockProducerStreamParams const &params)
{
    SourceExecStream::prepare(params);
    assert(params.outputTupleDesc.size() == 1);
    assert(!params.outputTupleDesc[0].isNullable);
    assert(StandardTypeDescriptor::isIntegralNative(
               StandardTypeDescriptorOrdinal(
                   params.outputTupleDesc[0].pTypeDescriptor->getOrdinal())));
    nRowsMax = params.nRows;
    TupleAccessor tupleAccessor;
    tupleAccessor.compute(params.outputTupleDesc);
    assert(tupleAccessor.isFixedWidth());
    cbTuple = tupleAccessor.getMaxByteCount();
}

void MockProducerStream::open(bool restart)
{
    SourceExecStream::open(restart);
    nRowsProduced = 0;
}

ExecStreamResult MockProducerStream::execute(ExecStreamQuantum const &)
{
    uint cbBatch = 0;
    PBuffer pBuffer = pOutAccessor->getProductionStart();
    uint cb = pOutAccessor->getProductionAvailable();
    while ((cb >= cbTuple) && (nRowsProduced < nRowsMax)) {
        cb -= cbTuple;
        cbBatch += cbTuple;
        ++nRowsProduced;
    }
    memset(pBuffer,0,cbBatch);
    if (cbBatch) {
        pOutAccessor->produceData(pBuffer + cbBatch);
        return EXECRC_OUTPUT;
    } else {
        if (nRowsProduced == nRowsMax) {
            pOutAccessor->markEOS();
            return EXECRC_EOS;
        }
        return EXECRC_NEED_OUTPUTBUF;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End MockProducerStream.cpp
