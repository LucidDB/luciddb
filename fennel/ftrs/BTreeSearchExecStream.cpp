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
#include "fennel/ftrs/BTreeSearchExecStream.h"
#include "fennel/btree/BTreeReader.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeSearchExecStream::prepare(BTreeSearchExecStreamParams const &params)
{
    BTreeReadExecStream::prepare(params);
    ConduitExecStream::prepare(params);
    
    outerJoin = params.outerJoin;

    // TODO:  assert inputDesc is a prefix of BTree key

    TupleDescriptor const &inputDesc = pInAccessor->getTupleDesc();

    TupleAccessor &inputAccessor = pInAccessor->getConsumptionTupleAccessor();

    if (params.inputKeyProj.size()) {
        inputKeyAccessor.bind(inputAccessor,params.inputKeyProj);
        inputKeyDesc.projectFrom(inputDesc,params.inputKeyProj);
    } else {
        inputKeyDesc = inputDesc;
    }
    inputKeyData.compute(inputKeyDesc);
    
    preFilterNulls = false;
    if (outerJoin && inputKeyDesc.containsNullable()) {
        // When we're doing an outer join, the input keys have not had
        // nulls eliminated yet, so we have to treat that case specially.
        preFilterNulls = true;
    }
    
    inputJoinAccessor.bind(inputAccessor,params.inputJoinProj);

    TupleDescriptor joinDescriptor;
    joinDescriptor.projectFrom(inputDesc,params.inputJoinProj);
    
    TupleProjection readerKeyProj = treeDescriptor.keyProjection;
    readerKeyProj.resize(inputKeyDesc.size());
    readerKeyAccessor.bind(
        pReader->getTupleAccessorForRead(),
        readerKeyProj);
    readerKeyData.compute(inputKeyDesc);
    
    nJoinAttributes = params.outputTupleDesc.size() - projAccessor.size();
}

void BTreeSearchExecStream::open(bool restart)
{
    BTreeReadExecStream::open(restart);
    ConduitExecStream::open(restart);
}

ExecStreamResult BTreeSearchExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }
    
    uint nTuples = 0;
    assert(quantum.nTuplesMax > 0);

    // outer loop
    for (;;) {

        if (!innerSearchLoop()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        // inner fetch loop
        for (;;) {
            if (nTuples >= quantum.nTuplesMax) {
                return EXECRC_QUANTUM_EXPIRED;
            }
            if (pOutAccessor->produceTuple(tupleData)) {
                ++nTuples;
            } else {
                return EXECRC_BUF_OVERFLOW;
            }
            if (pReader->searchNext()) {
                readerKeyAccessor.unmarshal(readerKeyData);
                int c = inputKeyDesc.compareTuples(
                    inputKeyData,readerKeyData);
                if (c == 0) {
                    // this is a match
                    projAccessor.unmarshal(
                        tupleData.begin() + nJoinAttributes);
                    // continue with inner fetch loop
                    continue;
                } else {
                    assert(c < 0);
                    // done with all matches
                }
            }
            pReader->endSearch();
            pInAccessor->consumeTuple();
            // break out of the inner loop, which will take us to the top of the
            // outer loop
            break;
        }
    }
}

bool BTreeSearchExecStream::innerSearchLoop()
{
    while (!pReader->isPositioned()) {
        if (!pInAccessor->demandData()) {
            return false;
        }
        TupleAccessor &inputAccessor =
            pInAccessor->accessConsumptionTuple();

        if (inputKeyAccessor.size()) {
            // unmarshal just the key projection
            inputKeyAccessor.unmarshal(inputKeyData);
        } else {
            // umarshal the whole thing as the key
            inputAccessor.unmarshal(inputKeyData);
        }

        bool found = pReader->searchForKey(inputKeyData,DUP_SEEK_BEGIN);
        if (preFilterNulls && found && inputKeyData.containsNull()) {
            // null never matches;
            // TODO:  so don't bother searching, but need a way
            // to fake pReader->isPositioned()
            found = false;
        }
            
        if (!found) {
            if (!outerJoin) {
                pInAccessor->consumeTuple();
                pReader->endSearch();
                continue;
            }
            // no match, so make up null values for the missing attributes
            for (uint i = nJoinAttributes; i < tupleData.size(); ++i) {
                tupleData[i].pData = NULL;
            }
        } else {
            projAccessor.unmarshal(
                tupleData.begin() + nJoinAttributes);
        }

        // propagate join attributes
        if (inputJoinAccessor.size()) {
            inputJoinAccessor.unmarshal(tupleData);
        }
    }
    return true;
}

void BTreeSearchExecStream::closeImpl()
{
    ConduitExecStream::closeImpl();
    BTreeReadExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeSearchExecStream.cpp
