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
#include "fennel/exec/CartesianJoinStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void CartesianJoinStream::prepare(CartesianJoinStreamParams const &params)
{
    ConfluenceExecStream::prepare(params);
    assert(inAccessors.size() == 2);
    pLeftBufAccessor = inAccessors[0];
    assert(pLeftBufAccessor);
    pRightBufAccessor = inAccessors[1];
    assert(pRightBufAccessor);
    TupleDescriptor const &leftDesc = pLeftBufAccessor->getTupleDesc();
    TupleDescriptor const &rightDesc = pRightBufAccessor->getTupleDesc();
    leftAccessor.compute(leftDesc);
    rightAccessor.compute(rightDesc);
    TupleDescriptor outputDesc;
    outputDesc.insert(outputDesc.end(),leftDesc.begin(),leftDesc.end());
    outputDesc.insert(outputDesc.end(),rightDesc.begin(),rightDesc.end());
    outputAccessor.compute(outputDesc);
    outputData.compute(outputDesc);
    pOutAccessor->setTupleShape(outputDesc);
}

void CartesianJoinStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);
    // these are used as state variables during fetch
    leftAccessor.resetCurrentTupleBuf();
    rightAccessor.resetCurrentTupleBuf();
}

ExecStreamResult CartesianJoinStream::execute(ExecStreamQuantum const &quantum)
{
    switch(pOutAccessor->getState()) {
    case EXECBUF_NEED_CONSUMPTION:
        return EXECRC_NEED_OUTPUTBUF;
    case EXECBUF_NEED_PRODUCTION:
    case EXECBUF_IDLE:
        break;
    case EXECBUF_EOS:
        assert(pLeftBufAccessor->getState() == EXECBUF_EOS);
        assert(pRightBufAccessor->getState() == EXECBUF_EOS);
        return EXECRC_EOS;
    }
    
    PBuffer pBuffer = pOutAccessor->getProductionStart();
    PBuffer pBufferEnd = pOutAccessor->getProductionEnd();
    PBuffer pNextTuple = pBuffer;

    // TODO:  lots of small optimizations possible here
    
    for (;;) {
        if (!leftAccessor.getCurrentTupleBuf()) {
            if (pLeftBufAccessor->getState() == EXECBUF_NEED_CONSUMPTION) {
                leftAccessor.setCurrentTupleBuf(
                    pLeftBufAccessor->getConsumptionStart());
                leftAccessor.unmarshal(outputData);
            } else {
                if (pNextTuple > pBuffer) {
                    pOutAccessor->produceData(pNextTuple);
                    return EXECRC_OUTPUT;
                }
                switch(pLeftBufAccessor->getState()) {
                case EXECBUF_EOS:
                    pOutAccessor->markEOS();
                    return EXECRC_EOS;
                case EXECBUF_IDLE:
                    pLeftBufAccessor->requestProduction();
                    return EXECRC_NEED_INPUT;
                case EXECBUF_NEED_PRODUCTION:
                    return EXECRC_NEED_INPUT;
                default:
                    permAssert(false);
                }
            }
        }
        for (;;) {
            if (!rightAccessor.getCurrentTupleBuf()) {
                if (pRightBufAccessor->getState() == EXECBUF_EOS) {
                    pLeftBufAccessor->consumeData(
                        pLeftBufAccessor->getConsumptionStart()
                        + leftAccessor.getCurrentByteCount());
                    leftAccessor.resetCurrentTupleBuf();
                    // restart right input stream
                    pGraph->getStreamInput(getStreamId(),1)->open(true);
                    // NOTE: break out of the inner for loop, which will take
                    // us back to the top of the outer for loop
                    break;
                }
                switch(pRightBufAccessor->getState()) {
                case EXECBUF_NEED_CONSUMPTION:
                    rightAccessor.setCurrentTupleBuf(
                        pRightBufAccessor->getConsumptionStart());
                    rightAccessor.unmarshal(outputData,leftAccessor.size());
                    break;
                case EXECBUF_IDLE:
                    pRightBufAccessor->requestProduction();
                    // NOTE:  fall through
                case EXECBUF_NEED_PRODUCTION:
                    if (pNextTuple > pBuffer) {
                        pOutAccessor->produceData(pNextTuple);
                        return EXECRC_OUTPUT;
                    }
                    return EXECRC_NEED_INPUT;
                default:
                    permAssert(false);
                }
            }
            if (!outputAccessor.isBufferSufficient(
                    outputData,pBufferEnd - pNextTuple))
            {
                assert(pNextTuple > pBuffer);
                pOutAccessor->produceData(pNextTuple);
                return EXECRC_OUTPUT;
            }
            outputAccessor.marshal(outputData,pNextTuple);
            pNextTuple += outputAccessor.getCurrentByteCount();
            pRightBufAccessor->consumeData(
                pRightBufAccessor->getConsumptionStart()
                + rightAccessor.getCurrentByteCount());
            rightAccessor.resetCurrentTupleBuf();
        }
    }
}

FENNEL_END_CPPFILE("$Id$");

// End CartesianJoinStream.cpp
