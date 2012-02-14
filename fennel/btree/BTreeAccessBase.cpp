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
#include "fennel/btree/BTreeAccessBaseImpl.h"
#include "fennel/btree/BTreeCompactNodeAccessor.h"
#include "fennel/btree/BTreeHeapNodeAccessor.h"
#include "fennel/btree/BTreeKeyedNodeAccessor.h"
#include "fennel/cache/CacheAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleOverflowExcn.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeAccessBase::BTreeAccessBase(BTreeDescriptor const &treeDescriptorInit)
    : treeDescriptor(treeDescriptorInit)
{
    keyDescriptor.projectFrom(
        treeDescriptor.tupleDescriptor,
        treeDescriptor.keyProjection);

    // TODO:  fine-tuning (in some cases fixed-width may be better for short
    // variable-width tuples, and indirection may be good for long fixed-width
    // tuples)

    // supported leaf accessor types
    typedef BTreeKeyedNodeAccessor<
        BTreeHeapNodeAccessor, TupleAccessor>
        VarNonLeafNodeAccessor;
    typedef BTreeKeyedNodeAccessor<
        BTreeCompactNodeAccessor, TupleAccessor>
        FixedNonLeafNodeAccessor;

    // supported non-leaf accessor types
    typedef BTreeKeyedNodeAccessor<
        BTreeHeapNodeAccessor, TupleProjectionAccessor>
        VarLeafNodeAccessor;
    typedef BTreeKeyedNodeAccessor<
        BTreeCompactNodeAccessor, TupleProjectionAccessor>
        FixedLeafNodeAccessor;

    // REVIEW:  These are just for deciding between fixed and var.  Add an
    // isFixedWidth() method to TupleDescriptor instead?
    TupleAccessor tmpLeafAccessor;
    tmpLeafAccessor.compute(treeDescriptor.tupleDescriptor);
    TupleAccessor tmpNonLeafAccessor;
    tmpNonLeafAccessor.compute(keyDescriptor);

    if (tmpLeafAccessor.isFixedWidth()) {
        FixedLeafNodeAccessor *pLeafNodeAccessorImpl =
            new FixedLeafNodeAccessor();
        pLeafNodeAccessor.reset(pLeafNodeAccessorImpl);
        pLeafNodeAccessorImpl->pKeyAccessor = &leafKeyAccessor;
    } else {
        VarLeafNodeAccessor *pLeafNodeAccessorImpl =
            new VarLeafNodeAccessor();
        pLeafNodeAccessor.reset(pLeafNodeAccessorImpl);
        pLeafNodeAccessorImpl->pKeyAccessor = &leafKeyAccessor;
    }

    if (tmpNonLeafAccessor.isFixedWidth()) {
        FixedNonLeafNodeAccessor *pNonLeafNodeAccessorImpl =
            new FixedNonLeafNodeAccessor();
        pNonLeafNodeAccessor.reset(pNonLeafNodeAccessorImpl);
        pNonLeafNodeAccessorImpl->pKeyAccessor =
            &(pNonLeafNodeAccessor->tupleAccessor);
    } else {
        VarNonLeafNodeAccessor *pNonLeafNodeAccessorImpl =
            new VarNonLeafNodeAccessor();
        pNonLeafNodeAccessor.reset(pNonLeafNodeAccessorImpl);
        pNonLeafNodeAccessorImpl->pKeyAccessor =
            &(pNonLeafNodeAccessor->tupleAccessor);
    }

    pLeafNodeAccessor->tupleDescriptor = treeDescriptor.tupleDescriptor;

    pNonLeafNodeAccessor->tupleDescriptor = keyDescriptor;
    StandardTypeDescriptorFactory stdTypeFactory;

    // TODO:  make PageId storage type selection automatic
    assert(sizeof(PageId) == sizeof(uint64_t));
    TupleAttributeDescriptor pageIdDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_UINT_64));
    pNonLeafNodeAccessor->tupleDescriptor.push_back(pageIdDesc);

    pLeafNodeAccessor->onInit();
    pNonLeafNodeAccessor->onInit();

    pChildAccessor = &(
        pNonLeafNodeAccessor->tupleAccessor.getAttributeAccessor(
            getKeyProjection().size()));

    leafKeyAccessor.bind(
        pLeafNodeAccessor->tupleAccessor,
        getKeyProjection());

    cbTupleMax =
        ((getSegment()->getUsablePageSize() - sizeof(BTreeNode)) / 2)
        - pLeafNodeAccessor->getEntryByteCount(0);
}

BTreeAccessBase::~BTreeAccessBase()
{
}

PageId BTreeAccessBase::getFirstChild(PageId parentPageId)
{
    BTreePageLock pageLock(treeDescriptor.segmentAccessor);
    while (parentPageId != NULL_PAGE_ID) {
        pageLock.lockShared(parentPageId);
        BTreeNode const &node = pageLock.getNodeForRead();
        assert(node.height);
        if (node.nEntries) {
            return getChild(node, 0);
        }
        parentPageId = node.rightSibling;
    }
    return NULL_PAGE_ID;
}

void BTreeAccessBase::setRootPageId(PageId rootPageId)
{
    treeDescriptor.rootPageId = rootPageId;
}

void BTreeAccessBase::validateTupleSize(TupleAccessor const &tupleAccessor)
{
    uint cbTuple = tupleAccessor.getCurrentByteCount();
    if (cbTuple > cbTupleMax) {
        TupleData tupleData(getTupleDescriptor());
        tupleAccessor.unmarshal(tupleData);
        throw TupleOverflowExcn(
            getTupleDescriptor(),
            tupleData,
            cbTuple,
            cbTupleMax);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeAccessBase.cpp
