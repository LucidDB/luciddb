/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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
#include "fennel/ftrs/BTreeReadExecStream.h"
#include "fennel/btree/BTreeReader.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeReadExecStream::prepare(BTreeReadExecStreamParams const &params)
{
    BTreeExecStream::prepare(params);
    outputProj.assign(params.outputProj.begin(), params.outputProj.end());
    tupleData.compute(params.outputTupleDesc);
}

void BTreeReadExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    BTreeExecStream::getResourceRequirements(minQuantity, optQuantity);

    // one page for BTreeReader
    minQuantity.nCachePages += 1;

    // TODO:  use opt to govern prefetch and come up with a good formula
    optQuantity = minQuantity;
}

void BTreeReadExecStream::open(bool restart)
{
    BTreeExecStream::open(restart);

    if (restart) {
        return;
    }

    // Create the reader here rather than during prepare, in case the btree
    // was dynamically created during stream graph open
    pReader = newReader();
    projAccessor.bind(
        pReader->getTupleAccessorForRead(),
        outputProj);
}

// TODO: When not projecting anything away, we could do producer buffer
// provision instead.  For BTreeCompactNodeAccessor, we can return multiple
// tuples directly by referencing the node data.  For other node accessor
// implementations, we can return single tuples by reference (although that's
// not always a win).

void BTreeReadExecStream::closeImpl()
{
    BTreeExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeReadExecStream.cpp
