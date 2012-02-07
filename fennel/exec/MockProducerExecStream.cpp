/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
    pBatchGenerator = params.pBatchGenerator;
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
    if (saveTuples || echoTuples) {
        assert(pGenerator);
    }
}

void MockProducerExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
    nRowsProduced = 0;
    savedTuples.clear();
    if (saveTuples) {
        // assume it's not too big
        savedTuples.reserve(nRowsMax);
    }
}

ExecStreamResult MockProducerExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (pGenerator) {
        TuplePrinter tuplePrinter;
        uint nTuples = 0;
        boost::scoped_array<int64_t> values(new int64_t[outputData.size()]);
        for (int col = 0; col < outputData.size(); ++col) {
            outputData[col].pData = reinterpret_cast<PConstBuffer>(
                &(values.get()[col]));
        }
        while (nRowsProduced < nRowsMax) {
            if (pOutAccessor->getProductionAvailable() < cbTuple) {
                return EXECRC_BUF_OVERFLOW;
            }

            if (pBatchGenerator) {
                int64_t newBatch = pBatchGenerator->next();
                if (newBatch == 0) {
                    return EXECRC_QUANTUM_EXPIRED;
                }
            }

            for (int col = 0; col < outputData.size(); ++col) {
                values.get()[col] =
                    pGenerator->generateValue(nRowsProduced, col);
            }

            bool rc = pOutAccessor->produceTuple(outputData);
            assert(rc);
            ++nTuples;
            ++nRowsProduced;
            if (echoTuples) {
                tuplePrinter.print(
                    *echoTuples,
                    pOutAccessor->getTupleDesc(), outputData);
            }
            if (saveTuples) {
                std::ostringstream oss;
                tuplePrinter.print(
                    oss, pOutAccessor->getTupleDesc(), outputData);
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
        memset(pBuffer, 0, cbBatch);
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

uint64_t MockProducerExecStream::getProducedRowCount()
{
    uint waitingRowCount = pOutAccessor->getConsumptionTuplesAvailable();
    return nRowsProduced - waitingRowCount;
}

MockProducerExecStreamGenerator::~MockProducerExecStreamGenerator()
{
}

FENNEL_END_CPPFILE("$Id$");

// End MockProducerExecStream.cpp
