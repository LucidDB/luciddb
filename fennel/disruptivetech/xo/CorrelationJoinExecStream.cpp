/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
// Portions Copyright (C) 1999-2007 John V. Sichi
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
#include "fennel/disruptivetech/xo/CorrelationJoinExecStream.h"
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
    outputDesc.insert(outputDesc.end(),leftDesc.begin(),leftDesc.end());
    outputDesc.insert(outputDesc.end(),rightDesc.begin(),rightDesc.end());
    outputData.compute(outputDesc);
    pOutAccessor->setTupleShape(outputDesc);

    nLeftAttributes = leftDesc.size();
    correlations.assign(params.correlations.begin(), 
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
            pGraph->getStreamInput(getStreamId(),1)->open(true);

            // make runnable
            if (++leftRowCount == 1) {
                for (std::vector<Correlation>::iterator it = correlations.begin();
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
std::cout << __LINE__ << std::endl;
                    pLeftBufAccessor->consumeTuple();
                    break;
                }
                if (!pRightBufAccessor->demandData()) {
std::cout << __LINE__ << std::endl;
                    return EXECRC_BUF_UNDERFLOW;
                }
std::cout << __LINE__ << std::endl;
                pRightBufAccessor->unmarshalTuple(
                    outputData, nLeftAttributes);
                break;
            }

std::cout << __LINE__ << std::endl;
            if (pOutAccessor->produceTuple(outputData)) {
#if 1
    TupleDescriptor statusDesc = pOutAccessor->getTupleDesc();
    TuplePrinter tuplePrinter;
    tuplePrinter.print(std::cout, statusDesc, outputData);
    std::cout << std::endl;
#endif

                ++nTuplesProduced;
            } else {
std::cout << __LINE__ << std::endl;
                return EXECRC_BUF_OVERFLOW;
            }
            
            pRightBufAccessor->consumeTuple();
            
            if (nTuplesProduced >= quantum.nTuplesMax) {
std::cout << __LINE__ << std::endl;
                return EXECRC_QUANTUM_EXPIRED;
            }
        }
    }
}

FENNEL_END_CPPFILE("$Id$");

// End CorrelationJoinExecStream.cpp
