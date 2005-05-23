/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
#include "fennel/xo/BTreeSearchUnique.h"
#include "fennel/btree/BTreeReader.h"
#include "fennel/common/ByteOutputStream.h"
#include "fennel/common/ByteInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

bool BTreeSearchUnique::writeResultToConsumerBuffer(
    ByteOutputStream &resultOutputStream)
{
    uint nJoinAttributes = outputDesc.size() - projAccessor.size();
    uint cbBuffer;
    PBuffer pBuffer = resultOutputStream.getWritePointer(1,&cbBuffer);
    PBuffer pBufferEnd = pBuffer + cbBuffer;
    PBuffer pNextTuple = pBuffer;
    ByteInputStream &inputResultStream =
        pInputStream->getProducerResultStream();
    for (;;) {
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
                // TODO:  assert unique by reading next key
            }

            // propagate join attributes
            if (inputJoinAccessor.size()) {
                inputJoinAccessor.unmarshal(tupleData);
            }
        }
        if (!outputAccessor.isBufferSufficient(
                tupleData,pBufferEnd - pNextTuple))
        {
            resultOutputStream.consumeWritePointer(pNextTuple - pBuffer);
            return true;
        }
        outputAccessor.marshal(tupleData,pNextTuple);
        pNextTuple += outputAccessor.getCurrentByteCount();
        assert(pNextTuple <= pBufferEnd);
        pReader->endSearch();
        inputResultStream.consumeReadPointer(
            inputAccessor.getCurrentByteCount());
    }
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeSearchUnique.cpp
