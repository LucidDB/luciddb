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
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void MockProducerExecStream::prepare(MockProducerExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);
    pGenerator = params.pGenerator;
    for (uint i = 0; i < params.outputTupleDesc.size(); i++) {
        assert(!params.outputTupleDesc[i].isNullable);
        StandardTypeDescriptorOrdinal ordinal =
            StandardTypeDescriptorOrdinal(
                params.outputTupleDesc[i].pTypeDescriptor->getOrdinal());
        assert(StandardTypeDescriptor::isIntegralNative(ordinal));
        if (pGenerator) {
            assert(ordinal == STANDARD_TYPE_INT_64);
            assert(i == 0);
        }
    }
    outputData.compute(params.outputTupleDesc);
    nRowsMax = params.nRows;
    TupleAccessor &tupleAccessor = pOutAccessor->getTraceTupleAccessor();
    assert(tupleAccessor.isFixedWidth());
    cbTuple = tupleAccessor.getMaxByteCount();
}

void MockProducerExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
    nRowsProduced = 0;
}

ExecStreamResult MockProducerExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (pGenerator) {
        uint nTuples = 0;
        int64_t value;
        outputData[0].pData = reinterpret_cast<PConstBuffer>(&value);
        while (nRowsProduced < nRowsMax) {
            if (pOutAccessor->getProductionAvailable() < cbTuple) {
                return EXECRC_BUF_OVERFLOW;
            }
            value = pGenerator->generateValue(nRowsProduced);
            bool rc = pOutAccessor->produceTuple(outputData);
            assert(rc);
            ++nTuples;
            ++nRowsProduced;
            if (nTuples >= quantum.nTuplesMax) {
                return EXECRC_QUANTUM_EXPIRED;
            }
        }
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }
    
    // NOTE: implementation below is kept lean and mean
    // intentionally so that it can be used to drive other streams with minimal
    // overhead during profiling

    uint cb = pOutAccessor->getProductionAvailable();
    uint nRows = std::min<uint64_t>(nRowsMax - nRowsProduced, cb / cbTuple);
    uint cbBatch = nRows * cbTuple;
    cb -= cbBatch;
    nRowsProduced += nRows;
    
    // TODO:  pOutAccessor->validateTupleSize(?);
    if (cbBatch) {
        PBuffer pBuffer = pOutAccessor->getProductionStart();
        memset(pBuffer,0,cbBatch);
        pOutAccessor->produceData(pBuffer + cbBatch);
        pOutAccessor->requestConsumption();
    } else {
        if (nRowsProduced == nRowsMax) {
            pOutAccessor->markEOS();
        }
    }
    if (nRowsProduced == nRowsMax) {
        return EXECRC_EOS;
    } else {
        return EXECRC_BUF_OVERFLOW;
    }
}

MockProducerExecStreamGenerator::~MockProducerExecStreamGenerator()
{
}

FENNEL_END_CPPFILE("$Id$");

// End MockProducerExecStream.cpp
