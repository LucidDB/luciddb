/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
    searchKeyParams = params.searchKeyParams;

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

            assert(
                searchKeyParams.size() == 0 ||
                (searchKeyParams.size() >= (n-1)*2+1 &&
                    searchKeyParams.size() <= n*2));
        } else {
            assert(searchKeyParams.size() == 0);
        }
        inputKeyAccessor.bind(inputAccessor,inputKeyProj);
        inputKeyDesc.projectFrom(inputDesc,inputKeyProj);
    } else {
        inputKeyDesc = inputDesc;
        assert(searchKeyParams.size() == 0);
    }
    inputKeyData.compute(inputKeyDesc);

    if (upperBoundDesc.size()) {
        // Verify that all the splitting above came out with the same
        // key type for both lower and upper bounds.
        assert(upperBoundDesc == inputKeyDesc);
    }

    preFilterNulls = false;
    if ((outerJoin && inputKeyDesc.containsNullable()) ||
        searchKeyParams.size() > 0)
    {
        // When we're doing an outer join or a lookup via dynamic parameters,
        // the input keys have not had nulls eliminated yet, so we have to
        // treat those cases specially.
        preFilterNulls = true;

        // Setup a projection of the search key.  In the case of a dynamic
        // parameter search, this will be done later when we read the
        // parameters
        if (searchKeyParams.size() == 0) {
            for (uint i = 0; i < inputKeyData.size(); i++) {
                searchKeyProj.push_back(i);
                upperBoundKeyProj.push_back(i);
            }
        }
    }

    inputJoinAccessor.bind(inputAccessor,params.inputJoinProj);

    TupleDescriptor joinDescriptor;
    joinDescriptor.projectFrom(inputDesc,params.inputJoinProj);

    TupleProjection readerKeyProj = treeDescriptor.keyProjection;
    readerKeyProj.resize(inputKeyDesc.size());
    readerKeyData.compute(inputKeyDesc);

    nJoinAttributes = params.outputTupleDesc.size() - params.outputProj.size();
}

void BTreeSearchExecStream::open(bool restart)
{
    // Read the parameter value of the btree's root page before
    // initializing a btree reader
    if (!restart && opaqueToInt(rootPageIdParamId) > 0) {
        treeDescriptor.rootPageId =
            *reinterpret_cast<PageId const *>(
            pDynamicParamManager->getParam(rootPageIdParamId).getDatum().pData);
    }
    BTreeReadExecStream::open(restart);
    ConduitExecStream::open(restart);
    dynamicKeysRead = false;

    if (restart) {
        return;
    }

    // Bind the accessor now that we've initialized the btree reader
    TupleProjection readerKeyProj = treeDescriptor.keyProjection;
    readerKeyProj.resize(inputKeyDesc.size());
    readerKeyAccessor.bind(
        pReader->getTupleAccessorForRead(),
        readerKeyProj);
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
        rc = innerFetchLoop(quantum, nTuples);
        if (rc == EXECRC_YIELD) {
            pInAccessor->consumeTuple();
        } else {
            return rc;
        }
    }
}

bool BTreeSearchExecStream::innerSearchLoop()
{
    while (!pReader->isPositioned()) {
        if (!pInAccessor->demandData()) {
            return false;
        }

        readSearchKey();
        readDirectives();
        pSearchKey = &inputKeyData;
        readUpperBoundKey();
        if (!searchForKey()) {
            pInAccessor->consumeTuple();
        }
    }
    return true;
}

void BTreeSearchExecStream::readSearchKey()
{
    // Even if we're not going to be reading the key values from the input
    // stream, we'll later need to read the directives, so we need to access
    // the input stream tuple
    TupleAccessor &inputAccessor =
        pInAccessor->accessConsumptionTuple();

    if (searchKeyParams.size() == 0) {
        if (inputKeyAccessor.size()) {
            // unmarshal just the key projection
            inputKeyAccessor.unmarshal(inputKeyData);
        } else {
            // umarshal the whole thing as the key
            inputAccessor.unmarshal(inputKeyData);
        }
    } else {
        // When passing in key values through dynamic parameters, only one
        // search range is allowed
        assert(!dynamicKeysRead);

        // NOTE zfong 5/22/07 - We are accessing the dynamic parameter values
        // by reference rather than value.  Therefore, the underlying values
        // are expected to be fixed for the duration of this search.  Likewise,
        // in readUpperBoundKey().
        uint nParams = searchKeyParams.size();
        searchKeyProj.clear();
        for (uint i = 0; i < nParams / 2; i++) {
            inputKeyData[searchKeyParams[i].keyOffset] =
                pDynamicParamManager->getParam(
                    searchKeyParams[i].dynamicParamId).getDatum();
            searchKeyProj.push_back(i);
        }
        // If there are an odd number of parameters, determine whether the
        // next parameter corresponds to the lower or upper bound
        if ((nParams%2) && searchKeyParams[nParams/2].keyOffset == nParams/2) {
            inputKeyData[nParams / 2] =
                pDynamicParamManager->getParam(
                    searchKeyParams[nParams / 2].dynamicParamId).getDatum();
            // The search key projection in the case of dynamic parameters
            // consists of only the portion of the search key that corresponds
            // to actual parameters supplied
            searchKeyProj.push_back(nParams / 2);
        }

        dynamicKeysRead = true;
    }
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

bool BTreeSearchExecStream::searchForKey()
{
    switch (lowerBoundDirective) {
    case SEARCH_UNBOUNDED_LOWER:
        if (pSearchKey->size() <= 1) {
            pReader->searchFirst();
            break;
        }
        // otherwise, this is the case where we have > 1 key and a
        // non-equality search on the last key; in this case, we need
        // to position to the equality portion of the key
    case SEARCH_CLOSED_LOWER:
        pReader->searchForKey(*pSearchKey, DUP_SEEK_BEGIN, leastUpper);
        break;
    case SEARCH_OPEN_LOWER:
        pReader->searchForKey(*pSearchKey, DUP_SEEK_END, leastUpper);
        break;
    default:
        permFail(
            "unexpected lower bound directive:  "
            << (char) lowerBoundDirective);
    }

    bool match = true;
    if (preFilterNulls && pSearchKey->containsNull(searchKeyProj)) {
        // null never matches when preFilterNulls is true;
        // TODO:  so don't bother searching, but need a way
        // to fake pReader->isPositioned()
        match = false;
    } else {
        if (pReader->isSingular()) {
            // Searched past end of tree.
            match = false;
        } else {
            if (preFilterNulls &&
                upperBoundData.containsNull(upperBoundKeyProj))
            {
                match = false;
            } else {
                match = testInterval();
            }
        }
    }

    if (!match) {
        if (!outerJoin) {
            pReader->endSearch();
            return false;
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

    return true;
}

void BTreeSearchExecStream::readUpperBoundKey()
{
    if (searchKeyParams.size() == 0) {
        if (upperBoundDesc.size()) {
            upperBoundAccessor.unmarshal(upperBoundData);
        } else {
            upperBoundData = *pSearchKey;
        }
    } else {
        // If there are an odd number of parameters, determine whether the
        // first parameter in the second group of parameters corresponds
        // to the lower or upper bound.  If there are an even number of
        // parameters, always read that first parameter.
        uint nParams = searchKeyParams.size();
        // The search key projection in the case of dynamic parameters
        // consists of only the portion of the search key that corresponds
        // to actual parameters supplied.  Since the lower and upper bound
        // keys may have a different number of supplied parameters, we need
        // to recreate the projection.
        upperBoundKeyProj.clear();
        if (!(nParams % 2)
            || searchKeyParams[nParams / 2].keyOffset == nParams / 2 + 1)
        {
            upperBoundData[0] =
                pDynamicParamManager->getParam(
                    searchKeyParams[nParams / 2].dynamicParamId).getDatum();
            upperBoundKeyProj.push_back(0);
        }
        uint keySize = upperBoundData.size();
        for (uint i = nParams / 2 + 1; i < nParams; i++) {
            upperBoundData[searchKeyParams[i].keyOffset - keySize] =
                pDynamicParamManager->getParam(
                    searchKeyParams[i].dynamicParamId).getDatum();
            upperBoundKeyProj.push_back(i - keySize);
        }
    }
}

bool BTreeSearchExecStream::testInterval()
{
    if (upperBoundDirective == SEARCH_UNBOUNDED_UPPER) {
        // if more than one search key in an unbounded search, the first part
        // of the key must be equality, so make sure that part of the key
        // matches
        if (pSearchKey->size() > 1) {
            readerKeyAccessor.unmarshal(readerKeyData);
            int c =
                inputKeyDesc.compareTuplesKey(
                    readerKeyData,
                    *pSearchKey,
                    pSearchKey->size() - 1);
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
                pSearchKey->size() > 1 && c > 0)
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

ExecStreamResult BTreeSearchExecStream::innerFetchLoop(
    ExecStreamQuantum const &quantum,
    uint &nTuples)
{
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
        // break out of this loop to enable a new key search
        return EXECRC_YIELD;
    }
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

FENNEL_END_CPPFILE("$Id$");

// End BTreeSearchExecStream.cpp
