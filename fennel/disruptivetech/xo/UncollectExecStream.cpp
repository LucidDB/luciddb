/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
#include "fennel/disruptivetech/xo/UncollectExecutionStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void UncollectExecutionStream::prepare(UncollectExecutionStreamParams const &params)
{
    ConduitExecStream::prepare(params);
}

void UncollectExecutionStream::open(bool restart) 
{
    ConduitExecStream::open(restart);
    //outputTupleData.compute(pOutAccessor->getTupleDesc());    
    //REVIEW: do we need to keep a seperate copy of the in buffer?
    //uint cbOutMaxsize = pInAccessor->getConsumptionTupleAccessor().getMaxByteCount();
    //pInputBuffer.reset(new FixedBuffer[cbOutMaxsize]);
    bytesWritten = 0;
}


ExecStreamResult UncollectExecutionStream::execute(ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (EXECRC_YIELD != rc) {
        return rc;
    }
        
    assert(1 == quantum.nTuplesMax);
    TupleData td;
    td.compute(pOutAccessor->getTupleDesc());
    while (bytesWritten <= pInAccessor->getConsumptionTupleAccessor().getMaxByteCount()) {
        // we havent read all of the in data yet
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        
        
        // write one item in the input array to the output buffer
        TupleAccessor& ta = pOutAccessor->getScratchTupleAccessor();
        ta.setCurrentTupleBuf(pInAccessor->getConsumptionTupleAccessor().getCurrentTupleBuf() + bytesWritten);
        ta.unmarshal(td);

        if (!pOutAccessor->produceTuple(td)) {
            return EXECRC_BUF_OVERFLOW;
        }

        bytesWritten += ta.getCurrentByteCount();

    }
    
    pInAccessor->consumeTuple();

    return EXECRC_QUANTUM_EXPIRED;
}

FENNEL_END_CPPFILE("$Id$");

// End UncollectExecutionStream.cpp
