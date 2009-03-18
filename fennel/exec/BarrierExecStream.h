/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

#include "fennel/common/FemEnums.h"
#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * BarrierExecStreamParams defines parameters for BarrierExecStream.
 */
struct BarrierExecStreamParams : public ConfluenceExecStreamParams
{
    /**
     * Return mode for the stream
     */
    BarrierReturnMode returnMode;

    /**
     * Ordered list of dynamic parameter ids
     */
    std::vector<DynamicParamId> parameterIds;
};

/**
 * BarrierExecStream is a synchronizing barrier to wait for the completion of
 * several upstream producers and generate a status output for the downstream
 * consumer.  The output returned by the barrier originates from its input.
 * The subset of data returned is determined by a parameter setting.  Barrier
 * may also optionally return the values specified by a list of dynamic
 * parameters that it reads.
 *
 * <p>
 * BarrierExecStream provides output buffer for its consumers.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class BarrierExecStream : public ConfluenceExecStream
{
    /**
     * Tupledata for input
     */
    TupleData inputTuple;

    /**
     * Whether output has been produced.
     */
    bool isDone;

    /**
     * Tuple used for sanity check on inputs
     */
    TupleData compareTuple;

    /**
     * A reference to the output accessor
     * contained in SingleOutputExecStream::pOutAccessor
     */
    TupleAccessor *outputTupleAccessor;

    /**
     * Total size of output buffer
     */
    uint outputBufSize;

    /**
     * buffer holding the outputTuple to provide to the consumers
     */
    boost::scoped_array<FixedBuffer> outputTupleBuffer;

    /**
     * 0-based ordinal of next input from which to read
     */
    uint iInput;

    /**
     * Mode that determines what the stream should return
     */
    BarrierReturnMode returnMode;

    /**
     * Current position within output buffer
     */
    uint curOutputPos;

    /**
     * Ordered list of dynamic parameters to be written into barrier's output
     * stream following the input stream data
     */
    std::vector<DynamicParamId> parameterIds;

    /**
     * Tupledata used to marshal dynamic parameter values to the output buffer
     */
    TupleData dynParamVal;

    /**
     * Processes the current input tuple
     */
    void processInputTuple();

    /**
     * Copies current input data into a buffer
     *
     * @param destBuffer buffer where the input data will be copied
     * @param pInAccessor current input stream buffer accessor
     *
     * @return number of bytes in the current input data
     */
    uint copyInputData(
        PBuffer destBuffer,
        SharedExecStreamBufAccessor &pInAccessor);

    /**
     * @return true if the only input data returned originates from the first
     * input stream
     */
    inline bool returnFirstInput();

    /**
     * @return true if all inputs into this stream must produce the same
     * value, one of which is returned by the stream
     */
    inline bool returnAnyInput();

    /**
     * @return true if all inputs' data are returned by this exec stream,
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

inline bool BarrierExecStream::returnFirstInput()
{
    return (returnMode == BARRIER_RET_FIRST_INPUT);
}

inline bool BarrierExecStream::returnAnyInput()
{
    return (returnMode == BARRIER_RET_ANY_INPUT);
}

inline bool BarrierExecStream::returnAllInputs()
{
    return (returnMode == BARRIER_RET_ALL_INPUTS);
}

FENNEL_END_NAMESPACE

#endif

// End BarrierExecStream.h
