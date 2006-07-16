/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 The Eigenbase Project
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
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/disruptivetech/calc/WinAggHistogramStrA.h"

FENNEL_BEGIN_CPPFILE("$Id$");

char* 
StringDesc::pointer() const
{
    assert(StandardTypeDescriptor::StandardTypeDescriptor::isArray(mType));
    assert(pData);
    return reinterpret_cast<char*>(const_cast<PBuffer>(pData));
}

TupleStorageByteLength
StringDesc::stringLength() const
{
    assert(StandardTypeDescriptor::isArray(mType));
    return (StandardTypeDescriptor::isVariableLenArray(mType)) ?
        cbData : cbStorage;
}

void WinAggHistogramStrA::addRow(RegisterRef<char*>* node)
{
    if (!node->isNull()) {
        StringDesc desc;
        desc.cbStorage = node->storage();
        desc.cbData = node->length();
        desc.mType = node->type();
        desc.pData = new FixedBuffer[desc.cbStorage];
        desc.memCopyFrom(*( node->getBinding()));
        
        (void) currentWindow.insert(desc);
    } else {
        ++nullRows;
    }
}

//! dropRow - Removes a value from the tree and updates
//! the running sum..
//!
//! Input - Value to be removed from the tree
//
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

        // Search the window for matching entries.  It may return more than one but
        // we will only delete one.
        assert(!currentWindow.empty());
        pair<winAggData::iterator, winAggData::iterator> entries =
            currentWindow.equal_range(desc);

        assert(entries.first != entries.second);  // should at least be one
        assert(NULL != entries.first->pData);
        
        if (entries.first != entries.second) {
            if (NULL != entries.first->pData) {
                delete [] entries.first->pData;
            }
            currentWindow.erase(entries.first);
        }
    } else {
        assert(0 != nullRows);
        --nullRows;
    }
}

//! getMin - Returns the current MIN() value for the window.
//!
//! Returns NULL if the window is empty.
void WinAggHistogramStrA::getMin(RegisterRef<char*>* node)
{
    if (0 != currentWindow.size()) {
        StringDesc minStr = *(currentWindow.begin());
        node->getBinding()->memCopyFrom(minStr);
    } else {
        // either all the rows added to the window had null
        // entries or there are no rows in the window.  Either
        // way the function returns NULL.
        node->toNull();
    }
}

//! getMax - Returns the current MAX() value for the window.
//!
//! Returns NULL if the window is empty.
void WinAggHistogramStrA::getMax(RegisterRef<char*>* node)
{
    if (0 != currentWindow.size()) {
        StringDesc maxStr = *(--(currentWindow.end()));
        node->getBinding(false)->memCopyFrom(maxStr);
    } else {
        // either all the rows added to the window had null
        // entries or there are no rows in the window.  Either
        // way the function returns NULL.
        node->toNull();
    }
}


FENNEL_END_CPPFILE("$Id$");

// End WinAggHistogramStrA.cpp
