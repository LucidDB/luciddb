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
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeSearchExecStream::prepare(BTreeSearchExecStreamParams const &params)
{
    BTreeReadExecStream::prepare(params);
    ConduitExecStream::prepare(params);
    
    leastUpper = true;
    outerJoin = params.outerJoin;

    // TODO:  assert inputDesc is a prefix of BTree key

    TupleDescriptor const &inputDesc = pInAccessor->getTupleDesc();

    TupleAccessor &inputAccessor = pInAccessor->getConsumptionTupleAccessor();

    if (params.inputDirectiveProj.size()) {
        assert(params.inputDirectiveProj.size() == 2);
        // If a directive is present, we must be projecting the keys, otherwise
        // the directives and keys would be overlapping, which doesn't make
        // sense.  Also, there should be an even number of keys, because
        // lower and upper bounds come together in the same tuple.
        assert(params.inputKeyProj.size() > 0);
        assert((params.inputKeyProj.size() % 2) == 0);
        directiveAccessor.bind(inputAccessor, params.inputDirectiveProj);
        TupleDescriptor inputDirectiveDesc;
        inputDirectiveDesc.projectFrom(inputDesc, params.inputDirectiveProj);

        // verify that the directive attribute has the correct datatype
        StandardTypeDescriptorFactory stdTypeFactory;
        TupleAttributeDescriptor expectedDirectiveDesc(
            stdTypeFactory.newDataType(STANDARD_TYPE_CHAR));
        expectedDirectiveDesc.cbStorage = 1;
        assert(
            inputDirectiveDesc[LOWER_BOUND_DIRECTIVE] == expectedDirectiveDesc);
        assert(
            inputDirectiveDesc[UPPER_BOUND_DIRECTIVE] == expectedDirectiveDesc);

        directiveData.compute(inputDirectiveDesc);
    }
    
    if (params.inputKeyProj.size()) {
        TupleProjection inputKeyProj = params.inputKeyProj;
        if (params.inputDirectiveProj.size()) {
            // The inputKeyProj gives us both lower and upper bounds;
            // split them because we will access them separately.
            TupleProjection upperBoundProj;
            int n = inputKeyProj.size() / 2;
            // This resize extends...
            upperBoundProj.resize(n);
            // ...so we have space to copy...
            std::copy(
                inputKeyProj.begin() + n,
                inputKeyProj.end(),
                upperBoundProj.begin());
            // ...whereas this one truncates what was copied.
            inputKeyProj.resize(n);

            upperBoundAccessor.bind(inputAccessor, upperBoundProj);
            upperBoundDesc.projectFrom(inputDesc, upperBoundProj);
            upperBoundData.compute(upperBoundDesc);
        }
        inputKeyAccessor.bind(inputAccessor,inputKeyProj);
        inputKeyDesc.projectFrom(inputDesc,inputKeyProj);
    } else {
        inputKeyDesc = inputDesc;
    }
    inputKeyData.compute(inputKeyDesc);

    if (upperBoundDesc.size()) {
        // Verify that all the splitting above came out with the same
        // key type for both lower and upper bounds.
        assert(upperBoundDesc == inputKeyDesc);
    }
    
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
            if (reachedTupleLimit(nTuples)) {
                return EXECRC_BUF_OVERFLOW;
            }
            if (pOutAccessor->produceTuple(tupleData)) {
                ++nTuples;
            } else {
                return EXECRC_BUF_OVERFLOW;
            }
            if (pReader->searchNext()) {
                if (testInterval()) {
                    projAccessor.unmarshal(
                        tupleData.begin() + nJoinAttributes);
                    // continue with inner fetch loop
                    continue;
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

        readDirectives();
        setAdditionalKeys();

        switch(lowerBoundDirective) {
        case SEARCH_UNBOUNDED_LOWER:
            if ((*pSearchKey).size() <= 1) {
                pReader->searchFirst();
                break;
            } 
            // otherwise, this is the case where we have > 1 key and a
            // non-equality search on the last key; in this case, we need
            // to position to the equality portion of the key
        case SEARCH_CLOSED_LOWER:
            pReader->searchForKey(*pSearchKey,DUP_SEEK_BEGIN,leastUpper);
            break;
        case SEARCH_OPEN_LOWER:
            pReader->searchForKey(*pSearchKey,DUP_SEEK_END,leastUpper);
            break;
        default:
            permFail(
                "unexpected lower bound directive:  "
                << (char) lowerBoundDirective);
        }

        bool match = true;
        if (preFilterNulls && (*pSearchKey).containsNull()) {
            // null never matches when preFilterNulls is true;
            // TODO:  so don't bother searching, but need a way
            // to fake pReader->isPositioned()
            match = false;
        } else {
            if (pReader->isSingular()) {
                // Searched past end of tree.
                match = false;
            } else {
                // Unmarshal upper bound key so we know where to stop
                // while scanning forward.
                if (upperBoundDesc.size()) {
                    upperBoundAccessor.unmarshal(upperBoundData);
                } else {
                    upperBoundData = *pSearchKey;
                }
            
                match = testInterval();
            }
        }
            
        if (!match) {
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

void BTreeSearchExecStream::readDirectives()
{
    if (!directiveAccessor.size()) {
        // default to point intervals
        lowerBoundDirective = SEARCH_CLOSED_LOWER;
        upperBoundDirective = SEARCH_CLOSED_UPPER;
        return;
    }
    
    directiveAccessor.unmarshal(directiveData);

    // directives can never be null
    assert(directiveData[LOWER_BOUND_DIRECTIVE].pData);
    assert(directiveData[UPPER_BOUND_DIRECTIVE].pData);
    
    lowerBoundDirective =
        SearchEndpoint(*(directiveData[LOWER_BOUND_DIRECTIVE].pData));
    upperBoundDirective =
        SearchEndpoint(*(directiveData[UPPER_BOUND_DIRECTIVE].pData));
}

bool BTreeSearchExecStream::testInterval()
{
    if (upperBoundDirective == SEARCH_UNBOUNDED_UPPER) {
        // if more than one search key in an unbounded search, the first part
        // of the key must be equality, so make sure that part of the key
        // matches
        if ((*pSearchKey).size() > 1) {
            readerKeyAccessor.unmarshal(readerKeyData);
            int c = inputKeyDesc.compareTuplesKey(
                readerKeyData, *pSearchKey, (*pSearchKey).size() - 1);
            if (c != 0) {
                return false;
            }
        }
        return true;
    } else {
        readerKeyAccessor.unmarshal(readerKeyData);
        int c = inputKeyDesc.compareTuples(upperBoundData, readerKeyData);
        if (upperBoundDirective == SEARCH_CLOSED_UPPER) {
            // if this is a greatest lower bound equality search on > 1 key,
            // it is possible that we are positioned one key to the left of
            // our desired key position; move forward one key to see if there
            // is a match
            if (!leastUpper && lowerBoundDirective == SEARCH_CLOSED_LOWER &&
                (*pSearchKey).size() > 1 && c > 0)
            {
                return checkNextKey();
            }
            if (c >= 0) {
                return true;
            }
        } else {
            if (c > 0) {
                return true;
            }
        }
    }
    return false;
}

bool BTreeSearchExecStream::checkNextKey()
{
    // read the next key
    if (!pReader->searchNext()) {
        return false;
    }
    readerKeyAccessor.unmarshal(readerKeyData);
    int c = inputKeyDesc.compareTuples(upperBoundData, readerKeyData);
    // should only have to read one more key
    assert(c <= 0);
    return (c == 0);
}

void BTreeSearchExecStream::closeImpl()
{
    ConduitExecStream::closeImpl();
    BTreeReadExecStream::closeImpl();
}

bool BTreeSearchExecStream::reachedTupleLimit(uint nTuples)
{
    return false;
}

void BTreeSearchExecStream::setAdditionalKeys()
{
    pSearchKey = &inputKeyData;
}

FENNEL_END_CPPFILE("$Id: //open/lu/dev/fennel/ftrs/BTreeSearchExecStream.cpp#7 $");

// End BTreeSearchExecStream.cpp
