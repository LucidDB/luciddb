/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
        tupleAccessor.setCurrentTupleBuf(
            getEntryForReadInline(node,iEntry));
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
        TupleData &scratchKey,
        bool &found)
    {
        uint probe = 0;
        uint base = probe;
        found = false;
        int nKeys = getKeyCount(node);
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
        if ((base != probe) && (base < node.nEntries)) {
            accessTupleInline(node,base);
        }
        return base;
    }

    virtual PConstBuffer getEntryForRead(BTreeNode const &node,uint iEntry)
    {
        return getEntryForReadInline(node,iEntry);
    }
};

FENNEL_END_NAMESPACE

#endif

// End BTreeKeyedNodeAccessor.h
