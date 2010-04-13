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
#include "fennel/btree/BTreeNonLeafReader.h"
#include "fennel/btree/BTreeAccessBaseImpl.h"
#include "fennel/btree/BTreeReaderImpl.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeNonLeafReader::BTreeNonLeafReader(BTreeDescriptor const &descriptor)
    : BTreeReader(descriptor)
{
}

bool BTreeNonLeafReader::adjustRootLockMode(LockMode &lockMode)
{
    assert(false);
    return false;
}

bool BTreeNonLeafReader::searchExtreme(bool first)
{
    assert(!isRootOnly());
    bool rc = searchExtremeInternal(first, READ_NONLEAF_ONLY);
    assert(rc);
    return rc;
}

bool BTreeNonLeafReader::searchForKey(
    TupleData const &key,
    DuplicateSeek dupSeek,
    bool leastUpper)
{
    assert(!isRootOnly());
    return
        searchForKeyInternal(
            key, dupSeek, leastUpper, getRootPageId(), rootLockMode,
            READ_NONLEAF_ONLY);
}

bool BTreeNonLeafReader::searchNext()
{
    assert(pageLock.isLocked());
    assert(pageLock.getNodeForRead().height == 1);
    return searchNextInternal();
}

TupleAccessor const &BTreeNonLeafReader::getTupleAccessorForRead() const
{
    return pNonLeafNodeAccessor->tupleAccessor;
}

bool BTreeNonLeafReader::isRootOnly()
{
    pageLock.lockPage(getRootPageId(), rootLockMode);
    BTreeNode const &node = pageLock.getNodeForRead();
    bool rc = (node.height == 0);
    pageLock.unlock();
    return rc;
}

bool BTreeNonLeafReader::isPositionedOnInfinityKey()
{
    assert(pageLock.isLocked());
    BTreeNode const &node = pageLock.getNodeForRead();
    return node.rightSibling == NULL_PAGE_ID
        && iTupleOnLowestLevel == node.nEntries - 1;
}

BTreeNodeAccessor &BTreeNonLeafReader::getNonLeafNodeAccessor()
{
    return *pNonLeafNodeAccessor;
}

PageId BTreeNonLeafReader::getChildForCurrent()
{
    return BTreeReader::getChildForCurrent();
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeNonLeafReader.cpp
