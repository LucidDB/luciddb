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
#include "fennel/btree/BTreeReader.h"
#include "fennel/btree/BTreeAccessBaseImpl.h"
#include "fennel/btree/BTreeReaderImpl.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeReader::BTreeReader(BTreeDescriptor const &descriptor)
    : BTreeAccessBase(descriptor)
{
    pageLock.accessSegment(treeDescriptor.segmentAccessor);
    pSearchKey = NULL;
    rootLockMode = LOCKMODE_S;
    nonLeafLockMode = LOCKMODE_S;
    leafLockMode = LOCKMODE_S;
    comparisonKeyData.compute(keyDescriptor);
    searchKeyData = comparisonKeyData;
    singular = true;
}

BTreeReader::~BTreeReader()
{
    endSearch();
}

bool BTreeReader::searchExtreme(bool first)
{
    return searchExtremeInternal(first, READ_ALL);
}

bool BTreeReader::searchExtremeInternal(bool first, ReadMode readMode)
{
    singular = false;
    pageId = getRootPageId();
    LockMode lockMode = rootLockMode;
    for (;;) {
        pageLock.lockPage(pageId, lockMode);
        BTreeNode const &node = pageLock.getNodeForRead();
        switch (node.height) {
        case 0:
            // at leaf level
            if (!adjustRootLockMode(lockMode)) {
                // Retry with correct lock mode
                continue;
            }
            if (!node.nEntries) {
                pageId = node.rightSibling;
                if (pageId == NULL_PAGE_ID) {
                    // FIXME jvs 11-Nov-2005:  see note in method documentation
                    // for searchLast.
                    singular = true;
                    return false;
                }
                continue;
            }
            if (first) {
                iTupleOnLowestLevel = 0;
            } else {
                iTupleOnLowestLevel = node.nEntries - 1;
            }
            accessLeafTuple();
            return true;
        case 1:
            if (readMode == READ_NONLEAF_ONLY) {
                if (first) {
                    iTupleOnLowestLevel = 0;
                } else {
                    iTupleOnLowestLevel = node.nEntries - 1;
                }
                accessTupleInline(node, iTupleOnLowestLevel);
                return true;
            }
            // next level down is leaf, so take the correct lock
            lockMode = leafLockMode;
            break;
        default:
            lockMode = nonLeafLockMode;
            break;
        }

        assert(node.nEntries);
        if (first) {
            // continue searching on first child
            pageId = getChild(node, 0);
        } else {
            // continue searching on last child
            pageId = getChild(node, node.nEntries - 1);
       }
    }
}

bool BTreeReader::searchNext()
{
    assert(pageLock.isLocked());
    assert(!pageLock.getNodeForRead().height);
    return searchNextInternal();
}

bool BTreeReader::searchNextInternal()
{
    assert(!singular);
    ++iTupleOnLowestLevel;
    for (;;) {
        BTreeNode const &node = pageLock.getNodeForRead();
        if (iTupleOnLowestLevel < node.nEntries) {
            accessTupleInline(node, iTupleOnLowestLevel);
            break;
        }
        pageId = node.rightSibling;
        if (pageId == NULL_PAGE_ID) {
            // might as well preserve position
            --iTupleOnLowestLevel;
            singular = true;
            return false;
        }
        pageLock.lockPage(pageId, leafLockMode);
        iTupleOnLowestLevel = 0;
    }
    return true;
}

bool BTreeReader::searchForKey(
    TupleData const &key, DuplicateSeek dupSeek, bool leastUpper)
{
    return
        searchForKeyInternal(
            key, dupSeek, leastUpper, getRootPageId(), rootLockMode, READ_ALL);
}

bool BTreeReader::searchForKeyInternal(
    TupleData const &key, DuplicateSeek dupSeek, bool leastUpper,
    PageId startPageId, LockMode initialLockMode, ReadMode readMode)
{
    singular = false;
    NullPageStack nullPageStack;
    bool found = searchForKeyTemplate<false, NullPageStack>(
        key, dupSeek, leastUpper, nullPageStack, startPageId, initialLockMode,
        readMode);
    pSearchKey = NULL;
    return found;
}

void BTreeReader::endSearch()
{
    pLeafNodeAccessor->tupleAccessor.resetCurrentTupleBuf();
    pageLock.unlock();
    singular = true;
}

TupleAccessor const &BTreeReader::getTupleAccessorForRead() const
{
    return pLeafNodeAccessor->tupleAccessor;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeReader.cpp
