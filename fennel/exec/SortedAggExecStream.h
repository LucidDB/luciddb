/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
struct FENNEL_EXEC_EXPORT SortedAggExecStreamParams
    : public ConduitExecStreamParams
{
    AggInvocationList aggInvocations;
    int groupByKeyCount;
    explicit SortedAggExecStreamParams()
    {
        groupByKeyCount = 0;
    }
};

/**
 * SortedAggExecStream aggregates its input, producing tuples of aggregate
 * function computations as output. It takes input sorted on a group key and
 * produce one output tuple per group.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT SortedAggExecStream
    : public ConduitExecStream
{
    enum State {
        STATE_ACCUMULATING,
        STATE_PRODUCING,
        STATE_DONE
    };

    State state;

    AggComputerList aggComputers;
    int groupByKeyCount;

    TupleData inputTuple;
    TupleDataWithBuffer prevTuple;
    TupleData outputTuple;
    bool prevTupleValid;

    inline void clearAccumulator();
    inline void updateAccumulator();
    inline void computeOutput();

    // Methods to store and compare group by keys
    inline void copyPrevGroupByKey();
    inline void setCurGroupByKey();
    inline int  compareGroupByKeys();
    inline ExecStreamResult produce();

protected:
    virtual AggComputer *newAggComputer(
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
