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
#include "fennel/disruptivetech/xo/CollectExecutionStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void CollectExecutionStream::prepare(CollectExecutionStreamParams const &params)
{
    ConduitExecStream::prepare(params);
}

void CollectExecutionStream::open(bool restart) 
{
    ConduitExecStream::open(restart);
    outputTupleData.compute(pOutAccessor->getTupleDesc());    
    uint cbOutMaxsize = pOutAccessor->getConsumptionTupleAccessor().getMaxByteCount();
    pOutputBuffer.reset(new FixedBuffer[cbOutMaxsize]);
    bytesWritten = 0;
}


ExecStreamResult CollectExecutionStream::execute(ExecStreamQuantum const &quantum)
{
    
    if (EXECBUF_EOS == pInAccessor->getState()) {
        // REVIEW wael 11/29-2004: Is it ok to return outbuf_overflow when we should(?) return EOS?
        TupleAccessor& ta = pOutAccessor->getScratchTupleAccessor();
        ta.setCurrentTupleBuf(pOutputBuffer.get());
        ta.unmarshal(outputTupleData);
        if (!pOutAccessor->produceTuple(outputTupleData)) {
            return EXECRC_BUF_OVERFLOW;
        }
    } 

    ExecStreamResult rc = precheckConduitBuffers();
    if (EXECRC_YIELD != rc) {
        return rc;
    }
        
    for (uint nTuples = 0; nTuples < quantum.nTuplesMax; ++nTuples) {
        while (!pInAccessor->isTupleConsumptionPending()) {
            if (!pInAccessor->demandData()) {
                return EXECRC_BUF_UNDERFLOW;
            }
            
            // write one input tuple to the staging output buffer
            memcpy(pOutputBuffer.get() + bytesWritten, 
                   pInAccessor->getConsumptionStart(),
                   pInAccessor->getScratchTupleAccessor().getCurrentByteCount());
            bytesWritten += pInAccessor->getScratchTupleAccessor().getCurrentByteCount();
        }

        
        pInAccessor->consumeTuple();
    }
    return EXECRC_QUANTUM_EXPIRED;
}

FENNEL_END_CPPFILE("$Id$");

// End CollectExecutionStream.cpp
