/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 1999-2005 John V. Sichi.
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
#include "fennel/disruptivetech/xo/CorrelationJoinExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"
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
    std::vector<Correlation>::iterator it = correlations.begin();
    for (/* empty */ ; it != correlations.end(); ++it) {
        pGraph->getDynamicParamManager().createParam(
                it->dynamicParamId,
                pLeftBufAccessor->getTupleDesc()[it->leftAttributeOrdinal]);
    }
}

void CorrelationJoinExecStream::close()
{
    std::vector<Correlation>::iterator it = correlations.begin();
    for (/* empty */ ; it != correlations.end(); ++it) {
        pGraph->getDynamicParamManager().removeParam(it->dynamicParamId);
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
                pGraph->getDynamicParamManager().setParam(
                    it->dynamicParamId, outputData[it->leftAttributeOrdinal]);
            }

            // restart right input stream
            pGraph->getStreamInput(getStreamId(),1)->open(true);
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
#if 0
    TupleDescriptor statusDesc = pOutAccessor->getTupleDesc();
    TuplePrinter tuplePrinter;
    tuplePrinter.print(std::cout, statusDesc, outputData);
    std::cout << std::endl;
#endif

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
