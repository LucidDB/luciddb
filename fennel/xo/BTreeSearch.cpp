/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
#include "fennel/xo/BTreeSearch.h"
#include "fennel/btree/BTreeReader.h"
#include "fennel/common/ByteOutputStream.h"
#include "fennel/common/ByteInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeSearch::prepare(BTreeSearchParams const &params)
{
    BTreeReadTupleStream::prepare(params);
    SingleInputTupleStream::prepare(params);
    
    outerJoin = params.outerJoin;

    // TODO:  assert inputDesc is a prefix of BTree key

    TupleDescriptor const &inputDesc = pInputStream->getOutputDesc();
    inputAccessor.compute(inputDesc);

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

    // for an outer join, output can be null
    if (outerJoin) {
        for (uint i = 0; i < outputDesc.size(); ++i) {
            outputDesc[i].isNullable = true;
        }
    }

    TupleDescriptor joinDescriptor;
    joinDescriptor.projectFrom(inputDesc,params.inputJoinProj);
    
    outputDesc.insert(
        outputDesc.begin(),
        joinDescriptor.begin(),
        joinDescriptor.end());

    outputAccessor.compute(outputDesc);
    tupleData.compute(outputDesc);

    TupleProjection readerKeyProj = treeDescriptor.keyProjection;
    readerKeyProj.resize(inputKeyDesc.size());
    readerKeyAccessor.bind(
        pReader->getTupleAccessorForRead(),
        readerKeyProj);
    readerKeyData.compute(inputKeyDesc);
}

void BTreeSearch::open(bool restart)
{
    BTreeReadTupleStream::open(restart);
    SingleInputTupleStream::open(restart);
}

bool BTreeSearch::writeResultToConsumerBuffer(
    ByteOutputStream &resultOutputStream)
{
    uint nJoinAttributes = outputDesc.size() - projAccessor.size();
    uint cbBuffer;
    PBuffer pBuffer = resultOutputStream.getWritePointer(1,&cbBuffer);
    PBuffer pBufferEnd = pBuffer + cbBuffer;
    PBuffer pNextTuple = pBuffer;
    ByteInputStream &inputResultStream =
        pInputStream->getProducerResultStream();

    // outer loop
    for (;;) {

        // inner search loop
        while (!pReader->isPositioned()) {
            PConstBuffer pTupleBuf = inputResultStream.getReadPointer(1);
            if (!pTupleBuf) {
                uint cb = pNextTuple - pBuffer;
                resultOutputStream.consumeWritePointer(cb);
                return (cb > 0);
            }
            inputAccessor.setCurrentTupleBuf(pTupleBuf);

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
                    inputResultStream.consumeReadPointer(
                        inputAccessor.getCurrentByteCount());
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

        // inner fetch loop
        for (;;) {
            if (!outputAccessor.isBufferSufficient(
                    tupleData,pBufferEnd - pNextTuple))
            {
                resultOutputStream.consumeWritePointer(pNextTuple - pBuffer);
                return true;
            }
            outputAccessor.marshal(tupleData,pNextTuple);
            pNextTuple += outputAccessor.getCurrentByteCount();
            assert(pNextTuple <= pBufferEnd);
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
            inputResultStream.consumeReadPointer(
                inputAccessor.getCurrentByteCount());
            // break out of the inner loop, which will take us to the top of the
            // outer loop
            break;
        }
    }
}

TupleDescriptor const &BTreeSearch::getOutputDesc() const
{
    return BTreeReadTupleStream::getOutputDesc();
}

void BTreeSearch::closeImpl()
{
    SingleInputTupleStream::closeImpl();
    BTreeReadTupleStream::closeImpl();
}

TupleStream::BufferProvision
BTreeSearch::getInputBufferRequirement() const
{
    return PRODUCER_PROVISION;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeSearch.cpp
