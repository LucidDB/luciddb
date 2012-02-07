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
#include "fennel/btree/BTreeCompactNodeAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeCompactNodeAccessor::BTreeCompactNodeAccessor()
{
    cbEntry = MAXU;
}

void BTreeCompactNodeAccessor::onInit()
{
    BTreeNodeAccessor::onInit();
    cbEntry = tupleAccessor.getMaxByteCount();
}

PBuffer BTreeCompactNodeAccessor::allocateEntry(
    BTreeNode &node,uint iEntry,uint)
{
    assert(iEntry < node.nEntries + 1);
    assert(node.cbTotalFree >= cbEntry);

    // shift everything over to make room for the new entry
    PBuffer pBuffer = node.getDataForWrite() + iEntry*cbEntry;
    memmove(
        pBuffer + cbEntry,
        pBuffer,
        (node.nEntries - iEntry)*cbEntry);

    // update node control info
    node.nEntries++;
    node.cbTotalFree -= cbEntry;
    return pBuffer;
}

void BTreeCompactNodeAccessor::deallocateEntry(
    BTreeNode &node,uint iEntry)
{
    assert(iEntry < node.nEntries);

    // NOTE: this test is to avoid passing an address beyond the end of the
    // page to memmove.  It should be unnecessary, since in that case the
    // number of bytes to be moved is 0, but paranoid memmove
    // implementations might complain.
    if (iEntry != node.nEntries - 1) {
        // shift over everything after the entry to delete it
        PBuffer pBuffer = node.getDataForWrite() + iEntry*cbEntry;
        memmove(
            pBuffer,
            pBuffer + cbEntry,
            (node.nEntries - (iEntry + 1))*cbEntry);
    }

    // update node control info
    node.nEntries--;
    node.cbTotalFree += cbEntry;
}

bool BTreeCompactNodeAccessor::hasFixedWidthEntries() const
{
    return true;
}

BTreeNodeAccessor::Capacity
BTreeCompactNodeAccessor::calculateCapacity(BTreeNode const &node,uint cbEntry)
{
    if (node.cbTotalFree >= cbEntry) {
        return CAN_FIT;
    } else {
        return CAN_NOT_FIT;
    }
}

uint BTreeCompactNodeAccessor::getEntryByteCount(uint cb)
{
    return cb;
}

void BTreeCompactNodeAccessor::compactNode(BTreeNode &,BTreeNode &)
{
    // Since we never return CAN_FIT_WITH_COMPACTION, no one should ever ask us
    // to do this.
    permAssert(false);
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeCompactNodeAccessor.cpp
