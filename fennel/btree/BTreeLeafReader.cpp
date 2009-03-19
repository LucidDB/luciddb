/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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
#include "fennel/btree/BTreeLeafReader.h"
#include "fennel/btree/BTreeAccessBaseImpl.h"
#include "fennel/btree/BTreeReaderImpl.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeLeafReader::BTreeLeafReader(BTreeDescriptor const &descriptor)
    : BTreeReader(descriptor)
{
    currLeafPageId = NULL_PAGE_ID;
}

bool BTreeLeafReader::adjustRootLockMode(LockMode &lockMode)
{
    assert(lockMode == leafLockMode);
    return true;
}

bool BTreeLeafReader::searchExtreme(bool first)
{
    assert(currLeafPageId != NULL_PAGE_ID);
    pageLock.lockPage(currLeafPageId, leafLockMode);
    BTreeNode const &node = pageLock.getNodeForRead();
    if (node.nEntries == 0) {
        singular = true;
        return false;
    }
    singular = false;
    if (first) {
        iTupleOnLowestLevel = 0;
    } else {
        iTupleOnLowestLevel = node.nEntries - 1;
    }
    accessLeafTuple();
    return true;
}

bool BTreeLeafReader::searchForKey(
    TupleData const &key,
    DuplicateSeek dupSeek,
    bool leastUpper)
{
    assert(currLeafPageId != NULL_PAGE_ID);
    return
        searchForKeyInternal(
            key, dupSeek, leastUpper, currLeafPageId, leafLockMode,
            READ_LEAF_ONLY);
}

bool BTreeLeafReader::searchNext()
{
    assert(currLeafPageId != NULL_PAGE_ID);
    assert(!singular);
    assert(pageLock.isLocked());
    assert(!pageLock.getNodeForRead().height);
    ++iTupleOnLowestLevel;
    BTreeNode const &node = pageLock.getNodeForRead();
    if (iTupleOnLowestLevel < node.nEntries) {
        accessLeafTuple();
        return true;
    } else {
        // might as well preserve position
        --iTupleOnLowestLevel;
        singular = true;
        return false;
    }
}

void BTreeLeafReader::endSearch()
{
    BTreeReader::endSearch();
    currLeafPageId = NULL_PAGE_ID;
}

void BTreeLeafReader::setCurrentPageId(PageId pageId)
{
    currLeafPageId = pageId;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeLeafReader.cpp
