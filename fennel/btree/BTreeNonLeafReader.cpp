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
