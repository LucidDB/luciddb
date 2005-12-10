/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_BTreeKeyedNodeAccessor_Included
#define Fennel_BTreeKeyedNodeAccessor_Included

#include "fennel/btree/BTreeNodeAccessor.h"

FENNEL_BEGIN_NAMESPACE

// TODO:  more doc

/**
 * BTreeKeyedNodeAccessor is a template for implementing some of the virtual
 * methods in the BTreeNodeAccessor interface.  It requires the class used to
 * instantiated NodeAccessor to implement a getEntryForReadInline method.
 */
template <class NodeAccessor,class KeyAccessor>
class BTreeKeyedNodeAccessor : public NodeAccessor
{
public:
    KeyAccessor *pKeyAccessor;

    void accessTupleInline(BTreeNode const &node,uint iEntry)
    {
        assert(iEntry < node.nEntries);
        NodeAccessor::tupleAccessor.setCurrentTupleBuf(
            NodeAccessor::getEntryForReadInline(node,iEntry));
    }
    
    virtual void accessTuple(BTreeNode const &node,uint iEntry)
    {
        return accessTupleInline(node,iEntry);
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
            accessTupleInline(node,probe);
            pKeyAccessor->unmarshal(scratchKey);
            int j = keyDescriptor.compareTuples(
                searchKey,scratchKey);
            if (j == 0) {
                found = true;
                switch(dupSeek) {
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
        if (!found && !leastUpper && base > 0)
            base--;
        if ((base != probe) && (base < node.nEntries)) {
            accessTupleInline(node,base);
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
        if (nKeys == 0)
            return -1;
        accessTupleInline(node, 0);
        pKeyAccessor->unmarshal(scratchKey);
        int compareResult = keyDescriptor.compareTuples(searchKey, scratchKey);
        return compareResult;
    }

    virtual PConstBuffer getEntryForRead(BTreeNode const &node,uint iEntry)
    {
        return NodeAccessor::getEntryForReadInline(node,iEntry);
    }
};

FENNEL_END_NAMESPACE

#endif

// End BTreeKeyedNodeAccessor.h
