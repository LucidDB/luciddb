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
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include <boost/scoped_array.hpp>


FENNEL_BEGIN_CPPFILE("$Id$");

MockProducerExecStream::MockProducerExecStream()
{
    cbTuple = 0;
    nRowsProduced = nRowsMax = 0;
    saveTuples = false;
    echoTuples = 0;
}

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
        }
    }
    outputData.compute(params.outputTupleDesc);
    TupleAccessor &tupleAccessor = pOutAccessor->getScratchTupleAccessor();
    assert(tupleAccessor.isFixedWidth());
    cbTuple = tupleAccessor.getMaxByteCount();
    nRowsMax = params.nRows;
    saveTuples = params.saveTuples;
    echoTuples = params.echoTuples;
    if (saveTuples||echoTuples) assert(pGenerator);
}

void MockProducerExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
    nRowsProduced = 0;
    savedTuples.clear();
    if (saveTuples) savedTuples.reserve(nRowsMax); // assume it's not too big
}

ExecStreamResult MockProducerExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (pGenerator) {
        TuplePrinter tuplePrinter; 
        uint nTuples = 0;
        boost::scoped_array<int64_t> values(new int64_t[outputData.size()]);
        for(int col=0;col<outputData.size();++col) {
            outputData[col].pData = reinterpret_cast<PConstBuffer>(&(values.get()[col]));
        }
        while (nRowsProduced < nRowsMax) {
            if (pOutAccessor->getProductionAvailable() < cbTuple) {
                return EXECRC_BUF_OVERFLOW;
            }
            
            for (int col=0;col<outputData.size();++col) {
                values.get()[col] = pGenerator->generateValue(nRowsProduced, col);
            }
            
            bool rc = pOutAccessor->produceTuple(outputData);
            assert(rc);
            ++nTuples;
            ++nRowsProduced;
            if (echoTuples)
                tuplePrinter.print(*echoTuples, pOutAccessor->getTupleDesc(), outputData);
            if (saveTuples) {
                std::ostringstream oss;
                tuplePrinter.print(oss, pOutAccessor->getTupleDesc(), outputData);
                savedTuples.push_back(oss.str());
            }
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
    
    // TODO:  pOutAccessor->validateTupleSize(?);
    if (cbBatch) {
        cb -= cbBatch;
        nRowsProduced += nRows;
        PBuffer pBuffer = pOutAccessor->getProductionStart();
        memset(pBuffer,0,cbBatch);
        pOutAccessor->produceData(pBuffer + cbBatch);
        pOutAccessor->requestConsumption();
    } 
    if (nRowsProduced == nRowsMax) {
        pOutAccessor->markEOS();
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
