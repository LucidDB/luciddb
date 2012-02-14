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
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/calculator/WinAggHistogramStrA.h"

FENNEL_BEGIN_CPPFILE("$Id$");

char*
StringDesc::pointer() const
{
    assert(StandardTypeDescriptor::isArray(mType));
    assert(pData);
    return reinterpret_cast<char*>(const_cast<PBuffer>(pData));
}

TupleStorageByteLength
StringDesc::stringLength() const
{
    assert(StandardTypeDescriptor::isArray(mType));
    return (StandardTypeDescriptor::isVariableLenArray(mType))
        ? cbData
        : cbStorage;
}

void WinAggHistogramStrA::addRow(RegisterRef<char*>* node)
{
    if (!node->isNull()) {
        StringDesc desc;
        desc.cbStorage = node->storage();
        desc.mType = node->type();
        desc.pData = new FixedBuffer[desc.cbStorage];
        desc.memCopyFrom(*node->getBinding());
        // printf(
        //     "WinAggHistogramStrA::addRow cbStorage=%d cbData=%d String=%s\n",
        //      node->storage(),desc.cbData, desc.pData);

        // Insert the value into the window
        (void) currentWindow.insert(desc);

        // Add to the FIFO queue. Another copy of the StringDesc is
        // created, but it points to the same memory.
        queue.push_back(desc);
    } else {
        ++nullRows;
    }
}

void WinAggHistogramStrA::dropRow(RegisterRef<char*>* node)
{
    if (!node->isNull()) {
        // create a StringDesc from the node data to supply to the
        // search routine
        StringDesc desc;
        desc.cbStorage = node->storage();
        desc.cbData = node->length();
        desc.mType = node->type();
        desc.pData = reinterpret_cast<PBuffer>(node->pointer());
//        printf(
//            "WinAggHistogramStrA::dropRow cbStorage=%d cbData=%d String=%s",
//            node->storage(),desc.cbData, desc.pData);

        // Search the window for matching entries.  It may return more
        // than one but we will only delete one.
        assert(!currentWindow.empty());
        pair<WinAggData::iterator, WinAggData::iterator> entries =
            currentWindow.equal_range(desc);

        assert(entries.first != entries.second);  // should at least be one
        assert(NULL != entries.first->pData);

        if (entries.first != entries.second) {
//             printf(
//                 "  erasing entry cbStorage=%d cbData=%d String=%s\n",
//                 entries.first->cbStorage,
//                 entries.first->cbData,
//                 entries.first->pData);
            if (NULL != entries.first->pData) {
                delete [] entries.first->pData;
            }
            currentWindow.erase(entries.first);
        }

        // Remove from the FIFO queue.
        queue.pop_front();
    } else {
        assert(0 != nullRows);
        --nullRows;
    }
}

void WinAggHistogramStrA::setReturnReg(
    RegisterRef<char*>* dest,
    const StringDesc& src)
{
    char* pData = dest->pointer();
    TupleStorageByteLength srcLength = src.stringLength();
    assert(pData);
    assert(srcLength <= dest->storage());
    memcpy(pData, src.pointer(), srcLength);
    dest->length(srcLength);
}


void WinAggHistogramStrA::getMin(RegisterRef<char*>* node)
{
    if (0 != currentWindow.size()) {
        setReturnReg(node, *(currentWindow.begin()));
    } else {
        // either all the rows added to the window had null
        // entries or there are no rows in the window.  Either
        // way the function returns NULL.
        node->toNull();
    }
}

void WinAggHistogramStrA::getMax(RegisterRef<char*>* node)
{
    if (0 != currentWindow.size()) {
        setReturnReg(node, *(--(currentWindow.end())));
    } else {
        // either all the rows added to the window had null
        // entries or there are no rows in the window.  Either
        // way the function returns NULL.
        node->toNull();
    }
}

void WinAggHistogramStrA::getFirstValue(RegisterRef<char*>* node)
{
    if (queue.empty()) {
        node->toNull();
    } else {
        setReturnReg(node, queue.front());
    }
}


void WinAggHistogramStrA::getLastValue(RegisterRef<char*>* node)
{
    if (queue.empty()) {
        node->toNull();
    } else {
        setReturnReg(node, queue.back());
    }
}

FENNEL_END_CPPFILE("$Id$");

// End WinAggHistogramStrA.cpp
