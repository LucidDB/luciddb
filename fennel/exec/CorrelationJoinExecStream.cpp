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
#include "fennel/exec/CorrelationJoinExecStream.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/tuple/TuplePrinter.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void CorrelationJoinExecStream::prepare(
    CorrelationJoinExecStreamParams const &params)
{
    assert(inAccessors.size() == 2);

    pLeftBufAccessor = inAccessors[0];
    assert(pLeftBufAccessor);

    pRightBufAccessor = inAccessors[1];
    assert(pRightBufAccessor);

    TupleDescriptor const &leftDesc = pLeftBufAccessor->getTupleDesc();
    TupleDescriptor const &rightDesc = pRightBufAccessor->getTupleDesc();

    TupleDescriptor outputDesc;
    outputDesc.insert(outputDesc.end(), leftDesc.begin(), leftDesc.end());
    outputDesc.insert(outputDesc.end(), rightDesc.begin(), rightDesc.end());
    outputData.compute(outputDesc);
    pOutAccessor->setTupleShape(outputDesc);

    nLeftAttributes = leftDesc.size();
    correlations.assign(
        params.correlations.begin(),
        params.correlations.end());
    //correlations.resize(correlations.size());
    //assert(correlations.size() > 0);
    assert(correlations.size() <= nLeftAttributes);

    ConfluenceExecStream::prepare(params);
}

void CorrelationJoinExecStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);

    if (!restart) {
        leftRowCount = 0;
        for (std::vector<Correlation>::iterator it = correlations.begin();
             it != correlations.end(); ++it)
        {
            pDynamicParamManager->createParam(
                it->dynamicParamId,
                pLeftBufAccessor->getTupleDesc()[it->leftAttributeOrdinal]);

            // Make right-hand child and its descendants (upstream XOs)
            // non-runnable. We don't want them to execute until we have
            // read a row from the left and called open(restart=true).
            const std::vector<ExecStreamId> &readerStreamIds =
                pGraph->getDynamicParamReaders(it->dynamicParamId);
            for (std::vector<ExecStreamId>::const_iterator it2 =
                     readerStreamIds.begin();
                 it2 != readerStreamIds.end(); ++it2)
            {
                pGraph->getScheduler()->setRunnable(
                    *pGraph->getStream(*it2), false);
            }
        }
    }
}

void CorrelationJoinExecStream::close()
{
    std::vector<Correlation>::iterator it = correlations.begin();
    for (/* empty */ ; it != correlations.end(); ++it) {
        pDynamicParamManager->deleteParam(it->dynamicParamId);
    }
    ConfluenceExecStream::closeImpl();
}

ExecStreamResult CorrelationJoinExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    // Note: implementation similar to CartesianJoinExecStream.
    uint nTuplesProduced = 0;

    for (;;) {
        if (!pLeftBufAccessor->isTupleConsumptionPending()) {
            if (pLeftBufAccessor->getState() == EXECBUF_EOS) {
                pOutAccessor->markEOS();
                return EXECRC_EOS;
            }
            if (!pLeftBufAccessor->demandData()) {
                return EXECRC_BUF_UNDERFLOW;
            }
            pLeftBufAccessor->unmarshalTuple(outputData);
            // updating the dynamic param(s) with the new left value(s)
            std::vector<Correlation>::iterator it = correlations.begin();
            for (/* empty */ ; it != correlations.end(); ++it) {
                pDynamicParamManager->writeParam(
                    it->dynamicParamId, outputData[it->leftAttributeOrdinal]);
            }

            // restart right input stream
            pGraph->getStreamInput(getStreamId(), 1)->open(true);

            // make runnable
            if (++leftRowCount == 1) {
                for (std::vector<Correlation>::iterator it =
                     correlations.begin();
                     it != correlations.end(); ++it)
                {
                    // Make the right-hand descendant that uses the
                    // variable runnable. Note that we made it
                    // non-runnable in open so that it didn't read an
                    // uninitialized variable.
                    const std::vector<ExecStreamId> &readerStreamIds =
                        pGraph->getDynamicParamReaders(it->dynamicParamId);
                    for (std::vector<ExecStreamId>::const_iterator it2 =
                             readerStreamIds.begin();
                         it2 != readerStreamIds.end(); ++it2)
                    {
                        pGraph->getScheduler()->setRunnable(
                            *pGraph->getStream(*it2), true);
                    }
                }
            }
        }
        for (;;) {
            if (!pRightBufAccessor->isTupleConsumptionPending()) {
                if (pRightBufAccessor->getState() == EXECBUF_EOS) {
                    pLeftBufAccessor->consumeTuple();
                    break;
                }
                if (!pRightBufAccessor->demandData()) {
                    return EXECRC_BUF_UNDERFLOW;
                }
                pRightBufAccessor->unmarshalTuple(
                    outputData, nLeftAttributes);
                break;
            }

            if (pOutAccessor->produceTuple(outputData)) {
                ++nTuplesProduced;
            } else {
                return EXECRC_BUF_OVERFLOW;
            }

            pRightBufAccessor->consumeTuple();

            if (nTuplesProduced >= quantum.nTuplesMax) {
                return EXECRC_QUANTUM_EXPIRED;
            }
        }
    }
}

FENNEL_END_CPPFILE("$Id$");

// End CorrelationJoinExecStream.cpp
