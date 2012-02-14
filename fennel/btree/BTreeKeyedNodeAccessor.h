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

#ifndef Fennel_BTreeKeyedNodeAccessor_Included
#define Fennel_BTreeKeyedNodeAccessor_Included

#include "fennel/btree/BTreeNodeAccessor.h"

FENNEL_BEGIN_NAMESPACE

// TODO:  more doc

/**
 * BTreeKeyedNodeAccessor is a template for implementing some of the virtual
 * methods in the BTreeNodeAccessor interface.  It requires the class used to
 * instantiate NodeAccessor to implement a getEntryForReadInline method.
 */
template <class NodeAccessor, class KeyAccessor>
class BTreeKeyedNodeAccessor : public NodeAccessor
{
public:
    KeyAccessor *pKeyAccessor;

    virtual void accessTupleInline(BTreeNode const &node,uint iEntry)
    {
        assert(iEntry < node.nEntries);
        NodeAccessor::tupleAccessor.setCurrentTupleBuf(
            NodeAccessor::getEntryForReadInline(node, iEntry));
    }

    virtual void accessTuple(BTreeNode const &node,uint iEntry)
    {
        return accessTupleInline(node, iEntry);
    }

    virtual void unmarshalKey(TupleData &keyData)
    {
        pKeyAccessor->unmarshal(keyData);
    }

    virtual uint binarySearch(
        BTreeNode const &node,
        TupleDescriptor const &keyDescriptor,
        TupleData const &searchKey,
        DuplicateSeek dupSeek,
        bool leastUpper,
        TupleData &scratchKey,
        bool &found)
    {
        uint probe = 0;
        uint base = probe;
        found = false;
        int nKeys = NodeAccessor::getKeyCount(node);
        while (nKeys > 0) {
            uint split = nKeys >> 1;
            probe = base + split;
            accessTupleInline(node, probe);
            pKeyAccessor->unmarshal(scratchKey);
            int j = keyDescriptor.compareTuples(
                searchKey, scratchKey);
            if (j == 0) {
                found = true;
                switch (dupSeek) {
                case DUP_SEEK_ANY:
                    return probe;
                case DUP_SEEK_BEGIN:
                    j = -1;
                    break;
                case DUP_SEEK_END:
                    j = 1;
                    break;
                default:
                    permAssert(false);
                }
            }
            if (j < 0) {
                nKeys = split;
            } else {
                base = probe + 1;
                nKeys -= (split + 1);
            }
        }
        if (!found && !leastUpper && (base > 0)) {
            base--;
        }
        if (((base != probe) && (base < node.nEntries))
            || ((node.nEntries == 1) && (node.height != 0)))
        {
            // one entry: +infinity
            accessTupleInline(node, base);
        }
        return base;
    }

    virtual int compareFirstKey(
        BTreeNode const &node,
        TupleDescriptor const &keyDescriptor,
        TupleData const &searchKey,
        TupleData &scratchKey)
    {
        int nKeys = NodeAccessor::getKeyCount(node);
        if (nKeys == 0) {
            return -1;
        }
        accessTupleInline(node, 0);
        pKeyAccessor->unmarshal(scratchKey);
        int compareResult = keyDescriptor.compareTuples(searchKey, scratchKey);
        return compareResult;
    }

    virtual PConstBuffer getEntryForRead(BTreeNode const &node,uint iEntry)
    {
        return NodeAccessor::getEntryForReadInline(node, iEntry);
    }
};

FENNEL_END_NAMESPACE

#endif

// End BTreeKeyedNodeAccessor.h
