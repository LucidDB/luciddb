/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
        pNonLeafNodeAccessor->tupleAccessor,childDatum);
    return *reinterpret_cast<PageId const *>(childDatum.pData);
}

inline PageId BTreeAccessBase::getChild(BTreeNode const &node,uint iChild)
{
    getNonLeafNodeAccessor(node).accessTuple(node,iChild);
    return getChildForCurrent();
}

inline PageId BTreeAccessBase::getRightSibling(PageId pageId)
{
    return getSegment()->getPageSuccessor(pageId);
}

inline void BTreeAccessBase::setRightSibling(
    BTreeNode &leftNode,PageId leftPageId,PageId rightPageId)
{
    getSegment()->setPageSuccessor(leftPageId,rightPageId);
    leftNode.rightSibling = rightPageId;
}

FENNEL_END_NAMESPACE

#endif

// End BTreeAccessBaseImpl.h
