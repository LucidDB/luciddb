/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/lcs/LcsClusterNode.h"
#include "fennel/lbm/LbmSearchExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmSearchExecStream::prepare(LbmSearchExecStreamParams const &params)
{
    BTreePrefetchSearchExecStream::prepare(params);

    rowLimitParamId = params.rowLimitParamId;
    ignoreRowLimit = (rowLimitParamId == DynamicParamId(0));
    if (!ignoreRowLimit) {
        // tupledatum for dynamic parameter
        rowLimitDatum.pData = (PConstBuffer) &rowLimit;
        rowLimitDatum.cbData = sizeof(rowLimit);
    }

    startRidParamId = params.startRidParamId;
    ridInKey = (startRidParamId > DynamicParamId(0));
    if (ridInKey) {
        startRidDatum.pData = (PConstBuffer) &startRid;
        startRidDatum.cbData = sizeof(startRid);

        // add on the rid to the btree search key if the key hasn't already
        // been setup
        TupleDescriptor ridKeyDesc = inputKeyDesc;
        if (inputKeyDesc.size() == treeDescriptor.keyProjection.size() - 1) {
            StandardTypeDescriptorFactory stdTypeFactory;
            TupleAttributeDescriptor attrDesc(
                stdTypeFactory.newDataType(STANDARD_TYPE_RECORDNUM));
            ridKeyDesc.push_back(attrDesc);
            ridKeySetup = false;
        } else {
            assert(
                inputKeyDesc.size() == 1
                && inputKeyDesc.size() == treeDescriptor.keyProjection.size());
            ridKeySetup = true;
        }

        savedLowerBoundAccessor.compute(ridKeyDesc);
        ridSearchKeyData.compute(ridKeyDesc);
        pfLowerBoundData.compute(ridKeyDesc);

        // need to look for greatest lower bound if searching on rid
        leastUpper = false;
    }
}

bool LbmSearchExecStream::reachedTupleLimit(uint nTuples)
{
    if (ignoreRowLimit) {
        return false;
    }

    // read the parameter the first time through
    if (nTuples == 0) {
        pDynamicParamManager->readParam(rowLimitParamId, rowLimitDatum);
    }
    // a row limit of 0 indicates that the scan should read till EOS
    if (rowLimit == 0) {
        return false;
    }
    return (nTuples >= rowLimit);
}

void LbmSearchExecStream::setAdditionalKeys()
{
    if (ridInKey) {
        // If the rid key was not setup in farrago, need to copy the keys
        // that precede the rid.  Also make sure that in this case, the search
        // is an equality one.  Otherwise, in the case where the key was setup,
        // the search is a greater than equal search.
        assert(lowerBoundDirective == SEARCH_CLOSED_LOWER);
        if (ridKeySetup) {
            assert(upperBoundDirective == SEARCH_UNBOUNDED_UPPER);
        } else {
            assert(upperBoundDirective == SEARCH_CLOSED_UPPER);

            for (uint i = 0; i < inputKeyData.size(); i++) {
                ridSearchKeyData[i] = inputKeyData[i];
            }
        }
        // rid is the last key; note that this needs to be reset each time
        // because the rid key value originates from a dynamic parameter
        // rather than the key buffer passed into setLowerBoundKey()
        ridSearchKeyData[ridSearchKeyData.size() - 1].pData =
            (PConstBuffer) &startRid;
        pDynamicParamManager->readParam(startRidParamId, startRidDatum);
        pSearchKey = &ridSearchKeyData;

    } else {
        pSearchKey = &inputKeyData;
    }
}

void LbmSearchExecStream::setLowerBoundKey(PConstBuffer buf)
{
    savedLowerBoundAccessor.setCurrentTupleBuf(buf);
    if (ridInKey) {
        savedLowerBoundAccessor.unmarshal(ridSearchKeyData);
        pSearchKey = &ridSearchKeyData;
    } else {
        savedLowerBoundAccessor.unmarshal(inputKeyData);
        pSearchKey = &inputKeyData;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LbmSearchExecStream.cpp
