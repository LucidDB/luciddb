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

const int ReturnAnyInput = -1;
const int ReturnAllInputs = -2;

/**
 * BarrierExecStreamParams defines parameters for BarrierExecStream.
 */
struct BarrierExecStreamParams : public ConfluenceExecStreamParams
{
    /**
     * If >= 0, the input stream that will return the row count that
     * BarrierExecStream produces.  If -1, all inputs return the same row
     * count.  If -2, return each input's row count as an output row, in
     * the order of the inputs.
     */
    int rowCountInput;   
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
     * Whether output has been produced.
     */
    bool isDone;    

    /**
     * Output tuple
     */
    TupleData outputTuple;

    /**
     * A reference to the output accessor 
     * contained in SingleOutputExecStream::pOutAccessor
     */
    TupleAccessor *outputTupleAccessor;

    /**
     * buffer holding the outputTuple to provide to the consumers
     */
    boost::scoped_array<FixedBuffer> outputTupleBuffer;

    /**
     * 0-based ordinal of next input from which to read
     */
    uint iInput;
    
    /**
     * Input containing row count output
     */
    int rowCountInput;

    /**
     * @return true if all inputs into this stream must produce the same
     * rowcount
     */
    inline bool returnAnyInput();

    /**
     * @return true if only one input's rowcount is returned by this exec
     * stream
     */
    inline bool returnOneInput();

    /**
     * @return true if all input's rowcounts are returned by this exec stream,
     * one per output row
     */
    inline bool returnAllInputs();

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

inline bool BarrierExecStream::returnAnyInput()
{
    return (rowCountInput == ReturnAnyInput);
}

inline bool BarrierExecStream::returnOneInput()
{
    return (rowCountInput >= 0);
}

inline bool BarrierExecStream::returnAllInputs()
{
    return (rowCountInput == ReturnAllInputs);
}

FENNEL_END_NAMESPACE

#endif

// End BarrierExecStream.h
