/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
    pReader = newReader();
    projAccessor.bind(
        pReader->getTupleAccessorForRead(),
        params.outputProj);
    tupleData.compute(params.outputTupleDesc);
}

void BTreeReadExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    BTreeExecStream::getResourceRequirements(minQuantity,optQuantity);
    
    // one page for BTreeReader
    minQuantity.nCachePages += 1;
    
    // TODO:  use opt to govern prefetch and come up with a good formula
    optQuantity = minQuantity;
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
