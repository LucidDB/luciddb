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

#ifndef Fennel_BTreeAccessBaseImpl_Included
#define Fennel_BTreeAccessBaseImpl_Included

#include "fennel/btree/BTreeAccessBase.h"
#include "fennel/btree/BTreeNodeAccessor.h"

FENNEL_BEGIN_NAMESPACE

inline BTreeNodeAccessor &BTreeAccessBase::getLeafNodeAccessor(
    BTreeNode const &node)
{
    assert(!node.height);
    return *pLeafNodeAccessor;
}

inline BTreeNodeAccessor &BTreeAccessBase::getNonLeafNodeAccessor(
    BTreeNode const &node)
{
    assert(node.height);
    return *pNonLeafNodeAccessor;
}

inline BTreeNodeAccessor &BTreeAccessBase::getNodeAccessor(
    BTreeNode const &node)
{
    if (node.height) {
        return *pNonLeafNodeAccessor;
    } else {
        return *pLeafNodeAccessor;
    }
}

inline PageId BTreeAccessBase::getChildForCurrent()
{
    TupleDatum &childDatum = pNonLeafNodeAccessor->tupleData.back();
    pChildAccessor->unmarshalValue(
        pNonLeafNodeAccessor->tupleAccessor, childDatum);
    return *reinterpret_cast<PageId const *>(childDatum.pData);
}

inline PageId BTreeAccessBase::getChild(BTreeNode const &node,uint iChild)
{
    getNonLeafNodeAccessor(node).accessTuple(node, iChild);
    return getChildForCurrent();
}

inline PageId BTreeAccessBase::getRightSibling(PageId pageId)
{
    return getSegment()->getPageSuccessor(pageId);
}

inline void BTreeAccessBase::setRightSibling(
    BTreeNode &leftNode,PageId leftPageId,PageId rightPageId)
{
    getSegment()->setPageSuccessor(leftPageId, rightPageId);
    leftNode.rightSibling = rightPageId;
}

FENNEL_END_NAMESPACE

#endif

// End BTreeAccessBaseImpl.h
