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

#ifndef Fennel_LbmAggExecStream_Included
#define Fennel_LbmAggExecStream_Included

#include "fennel/exec/SortedAggExecStream.h"
#include "fennel/exec/AggInvocation.h"
#include "fennel/exec/AggComputer.h"
#include "fennel/lucidera/bitmap/LbmByteSegment.h"
#include "fennel/tuple/TupleDataWithBuffer.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmSortedAggExecStreamParams defines parameters for LbmSortedAggExecStream.
 */
struct LbmSortedAggExecStreamParams : public SortedAggExecStreamParams
{
};

/**
 * LbmSortedAggExecStream aggregates its input, producing tuples with
 * aggregate computations as output. It takes input sorted on a group key
 * and produces one output tuple per group. It is similar to
 * SortedAggExecStream, but it's input consists of a bitmap tuple.
 * (group by keys, other fields, bitmap fields)
 *
 * <p>
 *
 * The behavior of MIN and MAX are not changed by bitmap fields.
 * However, the COUNT and SUM accumulations are applied multiple times,
 * according to the number of bits in the bitmap fields.
 *
 * @author John Pham
 * @version $Id$
 */
class LbmSortedAggExecStream : public SortedAggExecStream
{
protected:
    virtual AggComputer *newAggComputer(
        AggFunction aggFunction,
        TupleAttributeDescriptor const *pAttrDesc);

public:
    // implement ExecStream
    virtual void prepare(LbmSortedAggExecStreamParams const &params);
};

FENNEL_END_NAMESPACE

#endif

// End LbmSortedAggExecStream.h
