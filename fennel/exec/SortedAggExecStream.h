/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

#ifndef Fennel_SortedAggExecStream_Included
#define Fennel_SortedAggExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/exec/AggInvocation.h"
#include "fennel/exec/AggComputer.h"
#include "fennel/tuple/TupleDataWithBuffer.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SortedAggExecStreamParams defines parameters for SortedAggExecStream.
 */
struct SortedAggExecStreamParams : public ConduitExecStreamParams
{
    AggInvocationList aggInvocations;
};

/**
 * SortedAggExecStream aggregates its input, producing tuples of aggregate
 * function computations as output.  Currently it treats the entire input as
 * one group and so produces only a single tuple of output; it will soon be
 * extended to take input sorted on a group key and produce one output tuple
 * per group.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class SortedAggExecStream : public ConduitExecStream
{
    enum State {
        STATE_ACCUMULATING,
        STATE_PRODUCING,
        STATE_DONE
    };
    
    TupleData inputTuple;
    TupleDataWithBuffer accumulatorTuple;
    TupleDataWithBuffer outputTuple;
    State state;
    AggComputerList aggComputers;

    inline void clearAccumulator();
    inline void updateAccumulator();
    inline void computeOutput();

    AggComputer *newAggComputer(
        AggFunction aggFunction,
        TupleAttributeDescriptor const *pAttrDesc);
    
public:
    // implement ExecStream
    virtual void prepare(SortedAggExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End SortedAggExecStream.h
