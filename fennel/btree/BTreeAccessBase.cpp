/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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
