/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
#include "fennel/exec/BarrierExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");
void BarrierExecStream:: prepare(BarrierExecStreamParams const &params)
{
    TupleDescriptor outputTupleDesc;
    
    ConfluenceExecStream::prepare(params);

    /*
     * The output tuple from the producers should be the same: they all have
     * just one column recording number of rows processed by the producers.
     */
    outputTupleDesc = inAccessors[0]->getTupleDesc();

    // REVIEW jvs 27-Dec-2005:  more asserts needed according to above comment:
    // all inputs must match rowCount, and there must be at least one
    // input otherwise we'll produce garbage
    
    assert (outputTupleDesc.size() == 1);
    
    pOutAccessor->setTupleShape(outputTupleDesc);
    
    inputTuple.compute(outputTupleDesc);
    outputTuple.compute(outputTupleDesc);
    outputTuple[0].pData = (PConstBuffer) &rowCount;
    
}

void BarrierExecStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);
    iInput = 0;
}

ExecStreamResult BarrierExecStream::execute(ExecStreamQuantum const &)
{
    switch (pOutAccessor->getState()) {
    case EXECBUF_NONEMPTY:
    case EXECBUF_OVERFLOW:
        return EXECRC_BUF_OVERFLOW;
    case EXECBUF_UNDERFLOW:
    case EXECBUF_EMPTY:
        break;
    case EXECBUF_EOS:
        return EXECRC_EOS;
    }

    while (iInput < inAccessors.size()) {
        switch (inAccessors[iInput]->getState()) {
        case EXECBUF_OVERFLOW:
        case EXECBUF_NONEMPTY:
            inAccessors[iInput]->unmarshalTuple(inputTuple);
            if (iInput == 0) {
                /*
                 * First time, just copy the input into the output.
                 */
                rowCount = *reinterpret_cast<RecordNum const *>
                    (inputTuple[0].pData);
            } else {
                permAssert((inAccessors[iInput]->getTupleDesc()).
                            compareTuples(inputTuple, outputTuple) == 0);
            }
            inAccessors[iInput]->consumeTuple();
            // fall through
        case EXECBUF_UNDERFLOW:
            return EXECRC_BUF_UNDERFLOW;
        case EXECBUF_EMPTY:
            inAccessors[iInput]->requestProduction();
            return EXECRC_BUF_UNDERFLOW;
        case EXECBUF_EOS:
            ++iInput;
            break;
        default:
            permAssert(false);
        }
    }

    // attempt to write output
    bool success = pOutAccessor->produceTuple(outputTuple);
    if (success) {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    } else {
        return EXECRC_BUF_OVERFLOW;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End BarrierExecStream.cpp
