/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
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
#include "fennel/calculator/WinDistinctStrA.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void WinDistinctStrA::addRow(RegisterRef<char*>* node)
{
    if (!node->isNull()) {
//        printf("AddRow %s\n", node->pointer());
        StringDesc desc;
        desc.cbStorage = node->storage();
        desc.cbData = node->length();
        desc.mType = node->type();
        desc.pData = reinterpret_cast<PBuffer>(node->pointer());
        WinDistinctData::iterator it = currentWindow.find(desc);
        if (it == currentWindow.end()) {
            lastIsDistinct = true;
            desc.pData = new FixedBuffer[desc.cbStorage];
            desc.memCopyFrom(*node->getBinding());
            desc.cbData = node->length();
//            printf("Inserting %s(%d)\n", desc.pointer(), desc.stringLength());
            currentWindow[desc] = 1;
        } else {
            lastIsDistinct = false;
            (it->second)++;
        }
    }
}

void WinDistinctStrA::dropRow(RegisterRef<char*>* node)
{
    if (!node->isNull()) {
        // create a StringDesc from the node data to supply to the
        // search routine
        StringDesc desc;
        desc.cbStorage = node->storage();
        desc.cbData = node->length();
        desc.mType = node->type();
        desc.pData = reinterpret_cast<PBuffer>(node->pointer());
//        printf("DropRow %s(%d)\n", desc.pointer(), desc.stringLength());
        WinDistinctData::iterator it = currentWindow.find(desc);
        assert(it != currentWindow.end());
        if (it->second == 1) {
//            printf("Erasing\n");
            lastIsDistinct = true;
            PConstBuffer pData = it->first.pData;
            currentWindow.erase(it);
            if (pData != NULL) {
                delete [] pData;
            }
        } else {
            lastIsDistinct = false;
            (it->second)--;
        }
    }
}

FENNEL_END_CPPFILE("$Id$");

// End WinDistinctStrA.cpp
