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
#include "fennel/exec/CartesianJoinExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void CartesianJoinExecStream::prepare(
    CartesianJoinExecStreamParams const &params)
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
    
    ConfluenceExecStream::prepare(params);
}

ExecStreamResult CartesianJoinExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    // TODO:  lots of small optimizations possible here
    
    // TODO jvs 6-Nov-2004: one big optimization would be to perform
    // buffer-to-buffer joins instead of row-to-buffer joins.  This would
    // reduce the number of times the right input needs to be iterated by the
    // average number of rows in a buffer from the left input.  However,  the
    // output ordering would also be affected, so we might want to provide a
    // parameter to control this behavior.

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
        }
        for (;;) {
            if (!pRightBufAccessor->isTupleConsumptionPending()) {
                if (pRightBufAccessor->getState() == EXECBUF_EOS) {
                    pLeftBufAccessor->consumeTuple();
                    // restart right input stream
                    pGraph->getStreamInput(getStreamId(),1)->open(true);
                    // NOTE: break out of the inner for loop, which will take
                    // us back to the top of the outer for loop
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

// End CartesianJoinExecStream.cpp
