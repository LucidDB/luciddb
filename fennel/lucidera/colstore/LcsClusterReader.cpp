/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/lucidera/colstore/LcsClusterReader.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/tuple/TupleAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LcsClusterReader::LcsClusterReader(BTreeDescriptor &treeDescriptor) :
    LcsClusterAccessBase(treeDescriptor)
{
    bTreeReader = SharedBTreeReader(new BTreeReader(treeDescriptor));
}

LcsClusterNode const &LcsClusterReader::readClusterPage()
{
    bTreeReader->getTupleAccessorForRead().unmarshal(bTreeTupleData);
    clusterPageId = readClusterPageId();
    bTreeRid = readRid();
    clusterLock.lockShared(clusterPageId);
    LcsClusterNode const &node = clusterLock.getNodeForRead();
    return node;
}

bool LcsClusterReader::getFirstClusterPageForRead(
    PConstLcsClusterNode &pBlock)
{
    bool found;

    found = bTreeReader->searchFirst();
    if (!found)
        return false;

    LcsClusterNode const &node = readClusterPage();
    pBlock = &node;
    return true;
}

bool LcsClusterReader::getNextClusterPageForRead(PConstLcsClusterNode &pBlock)
{
    bool found;

    found = bTreeReader->searchNext();
    if (!found)
        return false;

    LcsClusterNode const &node = readClusterPage();
    pBlock = &node;
    return true;
}

FENNEL_END_CPPFILE("$Id$");

// End LcsClusterReader.cpp
