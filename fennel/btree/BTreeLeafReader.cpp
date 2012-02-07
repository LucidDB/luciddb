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
