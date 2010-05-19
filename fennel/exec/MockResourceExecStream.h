/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

#ifndef Fennel_MockResourceExecStream_Included
#define Fennel_MockResourceExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/segment/SegmentAccessor.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"

#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

struct FENNEL_EXEC_EXPORT MockResourceExecStreamParams
    : public SingleOutputExecStreamParams
{
    ExecStreamResourceQuantity minReqt;
    ExecStreamResourceQuantity optReqt;
    ExecStreamResourceSettingType optTypeInput;
    ExecStreamResourceQuantity expected;
};

/**
 * MockResourceExecStream is an exec stream that simply allocates scratch pages.
 * The stream takes as parameters minimum and optimum resource
 * requirements.  If it is able to allocate all pages that the resource
 * governor has assigned it, then 1 is written to the output stream, else
 * 0 is written.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT MockResourceExecStream
    : public SingleOutputExecStream
{
    ExecStreamResourceQuantity minReqt;
    ExecStreamResourceQuantity optReqt;
    ExecStreamResourceSettingType optTypeInput;
    ExecStreamResourceQuantity expected;

    /**
     * For allocating scratch pages
     */
    SegmentAccessor scratchAccessor;
    SegPageLock scratchLock;

    /**
     * Number of pages the resource manager indicates should be allocated
     */
    uint numToAllocate;

    /**
     * Tupledata for the output that indicates whether or not page allocation
     * was successful
     */
    TupleData outputTuple;
    TupleAccessor *outputTupleAccessor;
    boost::scoped_array<FixedBuffer> outputTupleBuffer;

    /**
     * True if page allocation completed
     */
    bool isDone;

public:
    // implement ExecStream
    virtual void prepare(MockResourceExecStreamParams const &params);
    virtual void open(bool restart);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity,
        ExecStreamResourceSettingType &optType);
    virtual void setResourceAllocation(ExecStreamResourceQuantity &quantity);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End MockResourceExecStream.h
