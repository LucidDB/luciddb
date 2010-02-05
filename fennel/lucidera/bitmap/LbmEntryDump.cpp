/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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
#include "fennel/lucidera/bitmap/LbmEntryDump.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LbmEntryDump::LbmEntryDump(
    TraceLevel traceLevelInit,
    SharedTraceTarget pTraceTargetInit,
    string nameInit)
    : TraceSource(pTraceTargetInit, nameInit)
{
    traceLevel = traceLevelInit;
}


uint LbmEntryDump::dump(BTreeDescriptor const &treeDescriptor, bool printRID)
{
    uint numTuples = 0;
    TupleData indexTuple;
    TupleDescriptor indexTupleDesc = treeDescriptor.tupleDescriptor;
    SharedBTreeReader pReader = SharedBTreeReader(
        new BTreeReader(treeDescriptor));

    TupleAccessor const& indexTupleAccessor =
        pReader->getTupleAccessorForRead();

    indexTuple.compute(indexTupleDesc);

    ostringstream treeRootPageId;
    treeRootPageId << "RootPageId " << opaqueToInt(treeDescriptor.rootPageId);
    FENNEL_TRACE(traceLevel, treeRootPageId.str());

    if (!pReader->searchFirst()) {
        pReader->endSearch();
    }

    while (pReader->isPositioned()) {
        indexTupleAccessor.unmarshal(indexTuple);
        FENNEL_TRACE(traceLevel, LbmEntry::toString(indexTuple, printRID));
        numTuples ++;

        if (!pReader->searchNext()) {
            pReader->endSearch();
            break;
        }
    }

    return numTuples;
}


FENNEL_END_CPPFILE("$Id$");

// End LbmEntryDump.cpp
