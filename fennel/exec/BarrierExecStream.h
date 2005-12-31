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

#ifndef Fennel_BarrierExecStream_Included
#define Fennel_BarrierExecStream_Included

#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * BarrierExecStreamParams defines parameters for BarrierExecStream.
 */
struct BarrierExecStreamParams : public ConfluenceExecStreamParams
{
    
};
    
/**
 * BarrierExecStream is a synchronizing barrier to wait for the completion of
 * several upstream producers and generate a status output for the downstream
 * consumer.
 * BarrierExecStream provides output buffer for its consumers.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class BarrierExecStream : public ConfluenceExecStream
{
    TupleData inputTuple;

    /**
     * Whether row count has been produced.
     */
    bool isDone;    

    /**
     * Output tuple which holds the row count.
     */
    TupleData outputTuple;

    /**
     * A reference to the output accessor 
     * contained in SingleOutputExecStream::pOutAccessor
     */
    TupleAccessor* outputTupleAccessor;

    /**
     * buffer holding the outputTuple to provide to the consumers
     */
    boost::scoped_array<FixedBuffer> outputTupleBuffer;

    /**
     * 0-based ordinal of next input from which to read
     */
    uint iInput;
    
    /**
     * Number of rows processed.
     */
    RecordNum rowCount;
    
    
public:
    // implement ExecStream
    virtual void prepare(BarrierExecStreamParams const &params);    
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual ExecStreamBufProvision getOutputBufProvision() const;
    /**
     * Implements ExecStream.
     */
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End BarrierExecStream.h
