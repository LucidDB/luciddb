/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/disruptivetech/xo/CollectExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/tuple/StandardTypeDescriptor.h"


FENNEL_BEGIN_CPPFILE("$Id$");

void CollectExecStream::prepare(CollectExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    FENNEL_TRACE(
        TRACE_FINER,
        "collect xo input TupleDescriptor = " 
        << pInAccessor->getTupleDesc());

    FENNEL_TRACE(
        TRACE_FINER,
        "collect xo output TupleDescriptor = " 
        << pOutAccessor->getTupleDesc());

    StandardTypeDescriptorOrdinal ordinal =
        StandardTypeDescriptorOrdinal(
        pOutAccessor->getTupleDesc()[0].pTypeDescriptor->getOrdinal());
    assert(ordinal == STANDARD_TYPE_VARBINARY);
    assert(1 == pOutAccessor->getTupleDesc().size());
}

void CollectExecStream::open(bool restart) 
{
    ConduitExecStream::open(restart);
    outputTupleData.compute(pOutAccessor->getTupleDesc());    
    inputTupleData.compute(pInAccessor->getTupleDesc());    

    uint cbOutMaxsize = pOutAccessor->getConsumptionTupleAccessor().getMaxByteCount();
    pOutputBuffer.reset(new FixedBuffer[cbOutMaxsize]);
    bytesWritten = 0;
    alreadyWrittenToOutput = false;
}

void CollectExecStream::close()
{
    pOutputBuffer.reset();
    ConduitExecStream::closeImpl();
}

ExecStreamResult CollectExecStream::execute(ExecStreamQuantum const &quantum)
{
    if (!alreadyWrittenToOutput && (EXECBUF_EOS == pInAccessor->getState())) {
        outputTupleData[0].pData = pOutputBuffer.get();
        outputTupleData[0].cbData = bytesWritten;
        if (!pOutAccessor->produceTuple(outputTupleData)) {
            return EXECRC_BUF_OVERFLOW;
        } 
        alreadyWrittenToOutput = true;
    } 

    ExecStreamResult rc = precheckConduitBuffers();
    if (EXECRC_YIELD != rc) {
        return rc;
    }
        
    for (uint nTuples = 0; nTuples < quantum.nTuplesMax; ++nTuples) {
        assert(!pInAccessor->isTupleConsumptionPending());
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }
  
        pInAccessor->unmarshalTuple(inputTupleData);
          
#if 0
    TupleDescriptor statusDesc = pInAccessor->getTupleDesc();
    TuplePrinter tuplePrinter;
    tuplePrinter.print(std::cout, statusDesc, inputTupleData);
    std::cout << std::endl;
#endif

        // write one input tuple to the staging output buffer
        memcpy(pOutputBuffer.get() + bytesWritten, 
               pInAccessor->getConsumptionStart(), 
               pInAccessor->getConsumptionTupleAccessor().getCurrentByteCount());
        bytesWritten += pInAccessor->getConsumptionTupleAccessor().getCurrentByteCount();
        pInAccessor->consumeTuple();
    }
    return EXECRC_QUANTUM_EXPIRED;
}

FENNEL_END_CPPFILE("$Id$");

// End CollectExecStream.cpp
